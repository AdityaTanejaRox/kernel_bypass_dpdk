// ============================================================
// rx_only.cpp
//
// A small DPDK benchmarking app with 2 main modes:
//
// 1) rx:     Poll RX queue and immediately free mbufs (baseline polling cost).
// 2) synth:  Synthetic alloc/free loop using DPDK mbuf pools (no NIC).
// 3) rx_ring: Producer polls NIC RX, enqueues mbufs into rte_ring,
//            multiple worker lcores dequeue + free + optional CPU work.
// ============================================================

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

// DPDK is C; include via extern "C" for C++ linkage.
extern "C" {
  #include <rte_common.h>    // (DPDK) common macros and compiler hints
  #include <rte_eal.h>       // (DPDK) EAL init, remote launch, lcore wait
  #include <rte_ethdev.h>    // (DPDK) ethdev: ports, rx/tx bursts, configure
  #include <rte_lcore.h>     // (DPDK) lcore ids, iteration macros, socket id
  #include <rte_mbuf.h>      // (DPDK) packet buffers (rte_mbuf) and mbuf pools
  #include <rte_cycles.h>    // (DPDK) rdtsc, tsc hz, timing helpers
  #include <rte_ring.h>      // (DPDK) lock-free ring between lcores
  #include <rte_pause.h>     // (DPDK) rte_pause() spin-wait hint
}

// ------------------------------------------------------------
// Global stop flag (set on SIGINT, also used to end loops cleanly)
static std::atomic<bool> g_stop{false};

// Global ring pointer used by rx_ring mode.
// rte_ring is a lock-free FIFO queue for passing pointers between lcores.
static rte_ring* g_rx_ring = nullptr;

// Ring size is number of pointer slots. 16384 is reasonable for bursts.
constexpr unsigned RX_RING_SIZE = 1 << 14; // 16384

// ------------------------------------------------------------
// SIGINT handler: stop the loops gracefully.
static void on_sigint(int) {
  g_stop.store(true, std::memory_order_relaxed);
}

// ============================================================
// AppConfig: all runtime knobs parsed from CLI (after EAL args)
// ============================================================
struct AppConfig {
  uint16_t port_id = 0;          // DPDK port id (NIC or vdev)
  uint16_t rx_queue_id = 0;      // RX queue id (we use 1 queue in this app)
  uint32_t duration_s = 10;      // benchmark duration
  uint16_t burst = 32;           // burst size for rx_burst/dequeue/enqueue
  uint32_t mbufs = 8192;         // mbufs per pool (or per-worker interpretation)
  uint32_t mbuf_cache = 256;     // per-lcore mempool cache size
  uint16_t rxd = 1024;           // RX descriptor ring size (queue depth)
  bool promisc = true;           // enable promiscuous mode
  bool latency = false;          // enable histogram sampling
  uint32_t sample_mask_pow2 = 0; // sample 1 out of 2^N bursts (0 => sample all)
  uint32_t work_nops = 0;        // synthetic CPU cost per packet/burst
  uint32_t workers = 1;          // number of worker lcores in synth/rx_ring
  std::string json_out;          // optional path to append JSONL results

  enum class PoolMode { PER_WORKER, SHARED };
  PoolMode pool = PoolMode::PER_WORKER;

  enum class Mode { RX, PCAP, SYNTH, RX_RING };
  Mode mode = Mode::RX;
};

// ============================================================
// Log2Hist: coarse latency histogram using log2 buckets.
// - bins[i] counts values in [2^i, 2^(i+1))
// - quantile() returns the lower bound of the bucket that crosses target
//
// This is intentionally cheap: good for "perf work" not perfect stats.
// ============================================================
struct Log2Hist {
  static constexpr int kBins = 64; // enough for huge ns values
  uint64_t bins[kBins]{};
  uint64_t samples = 0;

  // floor(log2(x)) using CLZ (count-leading-zeros).
  // Note: x must be > 0.
  static int bin_index(uint64_t x) {
    int idx = 63 - __builtin_clzll(x);
    if (idx < 0) idx = 0;
    if (idx >= kBins) idx = kBins - 1;
    return idx;
  }

  void merge_from(const Log2Hist& o) {
    for (int i = 0; i < kBins; ++i) bins[i] += o.bins[i];
    samples += o.samples;
  }

  static void print_range(uint64_t lb) {
    uint64_t ub = lb << 1;
    std::printf("[%" PRIu64 ", %" PRIu64 ") ns", lb, ub);
  }

  void add(uint64_t v) {
    if (v == 0) v = 1;            // avoid log2(0)
    bins[bin_index(v)]++;
    samples++;
  }

  // Approximate quantile from histogram. Returns bin lower bound.
  uint64_t quantile(double q) const {
    if (samples == 0) return 0;
    const uint64_t target = (uint64_t)(q * (double)samples);
    uint64_t acc = 0;
    for (int i = 0; i < kBins; ++i) {
      acc += bins[i];
      if (acc >= target) {
        return (1ULL << i);
      }
    }
    return (1ULL << (kBins - 1));
  }

  void print_ns() const {
    if (samples == 0) {
      std::printf("latency: no samples\n");
      return;
    }
    const uint64_t p50  = quantile(0.50);
    const uint64_t p95  = quantile(0.95);
    const uint64_t p99  = quantile(0.99);
    const uint64_t p999 = quantile(0.999);

    std::printf("latency (log2 bins): p50=");  print_range(p50);
    std::printf("  p95=");                      print_range(p95);
    std::printf("  p99=");                      print_range(p99);
    std::printf("  p999=");                     print_range(p999);
    std::printf("  samples=%" PRIu64 "\n", samples);
  }
};

// ============================================================
// WorkerResult: per worker counters + latency histogram.
// ============================================================
struct WorkerResult {
  uint64_t pkts = 0;   // packets processed
  uint64_t iters = 0;  // number of bursts processed
  Log2Hist hist{};
};

// ============================================================
// usage(): prints CLI help for app arguments (after "--").
// ============================================================
static void usage(const char* prog) {
  std::printf(
    "Usage: %s [EAL args...] -- "
    "--port <id> --queue <id> --duration <sec> --burst <n> "
    " ... --mode <rx|pcap|synth|rx_ring> ..."
    "--mbufs <n> --mbuf-cache <n> --rxd <n> [--no-promisc]\n",
    prog);
}

// ============================================================
// do_work_nops(n): simple synthetic CPU work (per packet/burst).
// This simulates parsing/logic cost in a tight loop.
// ============================================================
static inline void do_work_nops(uint32_t n) {
  for (uint32_t i = 0; i < n; ++i) {
    asm volatile("nop");
  }
}

// ============================================================
// parse_args(): parse app args AFTER EAL has consumed its part.
// Important: argv here should start with program name and app args.
// ============================================================
static bool parse_args(int argc, char** argv, AppConfig& cfg) {
  for (int i = 1; i < argc; i++) {
    auto need = [&](const char* name) -> const char* {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "Missing value for %s\n", name);
        return nullptr;
      }
      return argv[++i];
    };

    if (std::strcmp(argv[i], "--port") == 0) {
      const char* v = need("--port"); if (!v) return false;
      cfg.port_id = static_cast<uint16_t>(std::strtoul(v, nullptr, 10));
    }
    else if (std::strcmp(argv[i], "--queue") == 0) {
      const char* v = need("--queue"); if (!v) return false;
      cfg.rx_queue_id = static_cast<uint16_t>(std::strtoul(v, nullptr, 10));
    }
    else if (std::strcmp(argv[i], "--duration") == 0) {
      const char* v = need("--duration"); if (!v) return false;
      cfg.duration_s = static_cast<uint32_t>(std::strtoul(v, nullptr, 10));
    }
    else if (std::strcmp(argv[i], "--burst") == 0) {
      const char* v = need("--burst"); if (!v) return false;
      cfg.burst = static_cast<uint16_t>(std::strtoul(v, nullptr, 10));
    }
    else if (std::strcmp(argv[i], "--mbufs") == 0) {
      const char* v = need("--mbufs"); if (!v) return false;
      cfg.mbufs = static_cast<uint32_t>(std::strtoul(v, nullptr, 10));
    }
    else if (std::strcmp(argv[i], "--mbuf-cache") == 0) {
      const char* v = need("--mbuf-cache"); if (!v) return false;
      cfg.mbuf_cache = static_cast<uint32_t>(std::strtoul(v, nullptr, 10));
    }
    else if (std::strcmp(argv[i], "--rxd") == 0) {
      const char* v = need("--rxd"); if (!v) return false;
      cfg.rxd = static_cast<uint16_t>(std::strtoul(v, nullptr, 10));
    }
    else if (std::strcmp(argv[i], "--no-promisc") == 0) {
      cfg.promisc = false;
    }
    else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
      return false;
    }
    else if (std::strcmp(argv[i], "--latency") == 0) {
      cfg.latency = true;
    }
    else if (std::strcmp(argv[i], "--sample-mask") == 0) {
      const char* v = need("--sample-mask"); if (!v) return false;
      cfg.sample_mask_pow2 = static_cast<uint32_t>(std::strtoul(v, nullptr, 10));
    }
    else if (std::strcmp(argv[i], "--work-nops") == 0) {
      const char* v = need("--work-nops"); if (!v) return false;
      cfg.work_nops = static_cast<uint32_t>(std::strtoul(v, nullptr, 10));
    }
    else if (std::strcmp(argv[i], "--workers") == 0) {
      const char* v = need("--workers"); if (!v) return false;
      cfg.workers = static_cast<uint32_t>(std::strtoul(v, nullptr, 10));
      if (cfg.workers == 0) cfg.workers = 1;
    }
    else if (std::strcmp(argv[i], "--pool") == 0) {
      const char* v = need("--pool"); if (!v) return false;
      if (std::strcmp(v, "per_worker") == 0) cfg.pool = AppConfig::PoolMode::PER_WORKER;
      else if (std::strcmp(v, "shared") == 0) cfg.pool = AppConfig::PoolMode::SHARED;
      else {
        std::fprintf(stderr, "Invalid --pool (use per_worker|shared)\n");
        return false;
      }
    }
    else if (std::strcmp(argv[i], "--json-out") == 0) {
      const char* v = need("--json-out"); if (!v) return false;
      cfg.json_out = v;
    }
    else if (std::strcmp(argv[i], "--mode") == 0) {
      const char* v = need("--mode"); if (!v) return false;

      if      (std::strcmp(v, "rx")      == 0) cfg.mode = AppConfig::Mode::RX;
      else if (std::strcmp(v, "pcap")    == 0) cfg.mode = AppConfig::Mode::PCAP;
      else if (std::strcmp(v, "synth")   == 0) cfg.mode = AppConfig::Mode::SYNTH;
      else if (std::strcmp(v, "rx_ring") == 0) cfg.mode = AppConfig::Mode::RX_RING;
      else {
        std::fprintf(stderr, "Invalid --mode (use rx|pcap|synth|rx_ring)\n");
        return false;
      }
    }
    else {
      std::fprintf(stderr, "Unknown arg: %s\n", argv[i]);
      return false;
    }
  }
  return true;
}

// ============================================================
// RxStats: basic counters for rx loop.
// ============================================================
struct RxStats {
  uint64_t pkts = 0;
  uint64_t bursts = 0;
  uint64_t empty_polls = 0;
};

// ============================================================
// WorkerCtx: passed to each worker lcore.
// - cfg is copied so workers can read it without shared locking.
// - out points to the worker result storage.
// - shared_pool is used in synth mode when pool==SHARED.
// ============================================================
struct WorkerCtx {
  AppConfig cfg;
  WorkerResult* out;
  rte_mempool* shared_pool = nullptr;
};

// ============================================================
// synth_worker(): synthetic alloc/free benchmark (no NIC).
//
// Key DPDK calls:
// - rte_pktmbuf_pool_create()  [rte_mbuf.h]
// - rte_pktmbuf_alloc/free()   [rte_mbuf.h]
// - rte_get_tsc_hz(), rte_rdtsc() [rte_cycles.h]
// - rte_socket_id(), rte_lcore_id() [rte_lcore.h]
// ============================================================
static int synth_worker(void* arg) {
  auto* ctx = reinterpret_cast<WorkerCtx*>(arg);
  const AppConfig& cfg = ctx->cfg;
  WorkerResult& out = *ctx->out;

  // If shared_pool is provided, use it; otherwise create per-worker pool.
  rte_mempool* mbuf_pool = ctx->shared_pool;

  if (!mbuf_pool) {
    // rte_socket_id() [rte_lcore.h]
    // Returns NUMA socket of current lcore. Used for locality.
    const int socket_id = rte_socket_id();

    // rte_lcore_id() [rte_lcore.h]
    // Returns current lcore id (DPDK thread id).
    std::string pool_name =
        "MBUF_POOL_SYNTH_" + std::to_string(socket_id) + "_" + std::to_string(rte_lcore_id());

    // rte_pktmbuf_pool_create() [rte_mbuf.h]
    // Creates a mempool of mbufs: fixed-size packet buffers backed by hugepages.
    mbuf_pool = rte_pktmbuf_pool_create(
        pool_name.c_str(),
        cfg.mbufs,              // number of mbufs
        cfg.mbuf_cache,         // per-lcore cache size for allocation speed
        0,                      // private data size per mbuf (unused)
        RTE_MBUF_DEFAULT_BUF_SIZE, // data room size
        socket_id);             // NUMA socket

    if (!mbuf_pool) {
      // rte_errno + rte_strerror() gives the last DPDK error reason.
      std::fprintf(stderr, "[lcore %u] mempool create failed: %s\n",
                   rte_lcore_id(), rte_strerror(rte_errno));
      return -1;
    }
  }

  std::vector<rte_mbuf*> bufs(cfg.burst);

  // Timing helpers:
  // - rte_get_tsc_hz() [rte_cycles.h] => cycles/sec
  // - rte_rdtsc()      [rte_cycles.h] => read CPU TSC
  const uint64_t hz = rte_get_tsc_hz();
  const uint64_t t_start = rte_rdtsc();
  const uint64_t t_end_target = t_start + (uint64_t)cfg.duration_s * hz;

  while (!g_stop.load(std::memory_order_relaxed)) {
    const uint64_t now = rte_rdtsc();
    if (now >= t_end_target) break;

    // Allocate a burst of mbufs from the pool.
    uint16_t n = cfg.burst;
    for (uint16_t i = 0; i < n; ++i) {
      // rte_pktmbuf_alloc() [rte_mbuf.h]
      rte_mbuf* m = rte_pktmbuf_alloc(mbuf_pool);
      if (!m) { bufs[i] = nullptr; continue; }

      // rte_pktmbuf_mtod() [rte_mbuf.h]
      // "mbuf to data": pointer to packet payload storage.
      char* data = rte_pktmbuf_mtod(m, char*);
      if (data) data[0] = (char)0xAB;

      bufs[i] = m;
    }

    // Measure only the "work + free" section if latency enabled.
    const uint64_t t0 = rte_rdtsc();

    if (cfg.work_nops) {
      // Optional synthetic per-packet CPU cost.
      for (uint16_t i = 0; i < n; ++i) do_work_nops(cfg.work_nops);
    }

    // Free mbufs back to the pool.
    for (uint16_t i = 0; i < n; ++i) {
      if (bufs[i]) rte_pktmbuf_free(bufs[i]);
    }

    const uint64_t t1 = rte_rdtsc();

    out.iters++;
    out.pkts += n;

    if (cfg.latency) {
      // Convert cycles -> ns using 128-bit intermediate to prevent overflow.
      const uint64_t ns =
        (uint64_t)((__uint128_t)(t1 - t0) * 1000000000ULL / hz);
      out.hist.add(ns);
    }
  }

  return 0;
}

// ============================================================
// rx_producer_loop(): Producer for rx_ring pipeline.
// - polls NIC RX queue via rte_eth_rx_burst()
// - enqueues mbuf pointers into rte_ring for workers
//
// Key DPDK calls:
// - rte_eth_rx_burst()          [rte_ethdev.h]
// - rte_ring_enqueue_burst()    [rte_ring.h]
// - rte_pktmbuf_free()          [rte_mbuf.h]
// - rte_pause()                 [rte_pause.h]
// - rdtsc + hz                  [rte_cycles.h]
// ============================================================
static int rx_producer_loop(void* arg) {
  const AppConfig& cfg = *reinterpret_cast<AppConfig*>(arg);

  std::vector<rte_mbuf*> bufs(cfg.burst);

  const uint64_t hz = rte_get_tsc_hz();
  const uint64_t t_start = rte_rdtsc();
  uint64_t t_last = t_start;
  const uint64_t t_end = t_start + (uint64_t)cfg.duration_s * hz;

  uint64_t rx_pkts = 0;
  uint64_t enq_pkts = 0;
  uint64_t drop_pkts = 0;
  uint64_t empty_polls = 0;

  while (!g_stop.load(std::memory_order_relaxed)) {
    const uint64_t now = rte_rdtsc();
    if (now >= t_end) break;

    // rte_eth_rx_burst() [rte_ethdev.h]
    // Poll the RX queue and receive up to cfg.burst packets.
    const uint16_t n = rte_eth_rx_burst(
        cfg.port_id,
        cfg.rx_queue_id,
        bufs.data(),
        cfg.burst);

    if (n == 0) {
      // If nothing arrived, count it and pause (spin loop hint).
      empty_polls++;
      // rte_pause() [rte_pause.h]
      // CPU "pause" instruction: reduces power and helps hyperthread fairness.
      rte_pause();
      continue;
    }

    rx_pkts += n;

    // rte_ring_enqueue_burst() [rte_ring.h]
    // Enqueue mbuf pointers into lock-free ring.
    // Returns how many were enqueued (ring might be full).
    const unsigned enq = rte_ring_enqueue_burst(
        g_rx_ring,
        (void**)bufs.data(),
        n,
        nullptr);

    enq_pkts += enq;

    if (enq < n) {
      drop_pkts += (n - enq);

      // Drop overflow packets by freeing their mbufs.
      for (unsigned i = enq; i < n; ++i) {
        rte_pktmbuf_free(bufs[i]);
      }
    }

    // Progress once per ~1s
    const double since_last = double(now - t_last) / double(hz);
    if (since_last >= 1.0) {
      const double elapsed_s = double(now - t_start) / double(hz);
      const double rx_mpps   = (elapsed_s > 0) ? (double)rx_pkts  / elapsed_s / 1e6 : 0.0;
      const double enq_mpps  = (elapsed_s > 0) ? (double)enq_pkts / elapsed_s / 1e6 : 0.0;

      std::printf("producer t=%.1fs rx_pkts=%" PRIu64 " (%.3f Mpps) "
                  "enq_pkts=%" PRIu64 " (%.3f Mpps) drop=%" PRIu64 " empty_polls=%" PRIu64 "\n",
                  elapsed_s, rx_pkts, rx_mpps, enq_pkts, enq_mpps, drop_pkts, empty_polls);
      t_last = now;
    }
  }

  // Signal workers to exit.
  g_stop.store(true, std::memory_order_relaxed);

  const uint64_t t_end_real = rte_rdtsc();
  const double total_s = double(t_end_real - t_start) / double(hz);
  const double rx_mpps  = (total_s > 0) ? (double)rx_pkts  / total_s / 1e6 : 0.0;
  const double enq_mpps = (total_s > 0) ? (double)enq_pkts / total_s / 1e6 : 0.0;

  std::printf("\nproducer DONE\n");
  std::printf("producer duration=%.3fs rx_pkts=%" PRIu64 " rx_mpps=%.3f "
              "enq_pkts=%" PRIu64 " enq_mpps=%.3f drop_pkts=%" PRIu64 " empty_polls=%" PRIu64 "\n",
              total_s, rx_pkts, rx_mpps, enq_pkts, enq_mpps, drop_pkts, empty_polls);

  return 0;
}

// ============================================================
// rx_worker_loop(): Consumer workers for rx_ring pipeline.
// - dequeue mbufs from rte_ring
// - optional per-packet CPU work
// - free mbufs
// - optional sampling for latency hist
//
// Key DPDK calls:
// - rte_ring_dequeue_burst()   [rte_ring.h]
// - rte_pktmbuf_free()         [rte_mbuf.h]
// - rte_pause()                [rte_pause.h]
// - rdtsc + hz                 [rte_cycles.h]
// ============================================================
static int rx_worker_loop(void* arg) {
  auto* ctx = reinterpret_cast<WorkerCtx*>(arg);
  const AppConfig& cfg = ctx->cfg;
  WorkerResult& out = *ctx->out;

  std::vector<rte_mbuf*> bufs(cfg.burst);

  const uint64_t hz = rte_get_tsc_hz();
  uint64_t burst_idx = 0;

  // sample_mask_pow2=N => sample every 2^N bursts.
  // Example: N=0 => mask=0 => sample every burst.
  const uint64_t sample_mask =
      (cfg.sample_mask_pow2 >= 63) ? ~0ULL : ((1ULL << cfg.sample_mask_pow2) - 1ULL);

  while (!g_stop.load(std::memory_order_relaxed)) {
    // rte_ring_dequeue_burst() [rte_ring.h]
    // Dequeue up to cfg.burst pointers from ring.
    const uint16_t n = (uint16_t)rte_ring_dequeue_burst(
        g_rx_ring,
        (void**)bufs.data(),
        cfg.burst,
        nullptr);

    if (n == 0) {
      rte_pause();
      continue;
    }

    // Optional sampling (to keep histogram overhead bounded).
    const bool do_sample = cfg.latency && ((burst_idx & sample_mask) == 0);
    uint64_t t0 = 0;
    if (do_sample) t0 = rte_rdtsc();

    // Optional synthetic CPU work per packet (simulate parsing / risk checks / etc.)
    if (cfg.work_nops) {
      for (uint16_t i = 0; i < n; ++i) {
        do_work_nops(cfg.work_nops);
      }
    }

    // Free packets back to mempool
    for (uint16_t i = 0; i < n; ++i) {
      rte_pktmbuf_free(bufs[i]);
    }

    if (do_sample) {
      const uint64_t t1 = rte_rdtsc();
      const uint64_t ns =
        (uint64_t)((__uint128_t)(t1 - t0) * 1000000000ULL / hz);
      out.hist.add(ns);
    }

    out.pkts += n;
    out.iters++;
    burst_idx++;
  }

  return 0;
}

// ============================================================
// init_port_rx(): Configure & start an RX-only port with 1 RX queue.
//
// Key DPDK calls:
// - rte_eth_dev_count_avail()     [rte_ethdev.h]
// - rte_eth_dev_info_get()        [rte_ethdev.h]
// - rte_eth_dev_is_valid_port()   [rte_ethdev.h]
// - rte_eth_dev_configure()       [rte_ethdev.h]
// - rte_eth_rx_queue_setup()      [rte_ethdev.h]
// - rte_eth_dev_start()           [rte_ethdev.h]
// - rte_eth_promiscuous_enable()  [rte_ethdev.h]
// - rte_eth_link_get_nowait()     [rte_ethdev.h]
// ============================================================
static int init_port_rx(const AppConfig& cfg, rte_mempool* mbuf_pool) {
  constexpr uint16_t kRxQueues = 1;
  constexpr uint16_t kTxQueues = 0;

  // rte_eth_dev_count_avail() [rte_ethdev.h]
  // How many ethdev ports are available (physical NICs or vdevs).
  const uint16_t n_ports = rte_eth_dev_count_avail();

  std::printf("DPDK ports available: %u\n", n_ports);
  for (uint16_t p = 0; p < n_ports; ++p) {
    // rte_eth_dev_info_get() [rte_ethdev.h]
    // Query driver name, queue limits, if_index (if supported), offloads, etc.
    rte_eth_dev_info info{};
    rte_eth_dev_info_get(p, &info);
    std::printf("  port %u: driver=%s if_index=%u\n",
                p,
                info.driver_name ? info.driver_name : "(null)",
                info.if_index);
  }

  // rte_eth_dev_is_valid_port() [rte_ethdev.h]
  if (!rte_eth_dev_is_valid_port(cfg.port_id)) {
    std::fprintf(stderr, "Invalid port id: %u (ports available=%u)\n", cfg.port_id, n_ports);
    return -1;
  }

  // rte_eth_conf: port configuration structure.
  // Here we keep it minimal: single RX queue, no RSS, no fancy offloads.
  rte_eth_conf port_conf{};
  port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_NONE;

  // rte_eth_dev_configure() [rte_ethdev.h]
  // Configure number of RX/TX queues and basic port behavior.
  int rc = rte_eth_dev_configure(cfg.port_id, kRxQueues, kTxQueues, &port_conf);
  if (rc < 0) {
    std::fprintf(stderr, "rte_eth_dev_configure failed: %s\n", rte_strerror(-rc));
    return -1;
  }

  // rte_eth_rx_queue_setup() [rte_ethdev.h]
  // Configure RX descriptor ring (queue). mbuf_pool supplies mbufs for RX.
  rc = rte_eth_rx_queue_setup(
      cfg.port_id,
      cfg.rx_queue_id,
      cfg.rxd,            // descriptor ring size
      rte_socket_id(),    // NUMA socket
      nullptr,            // use default rx_conf
      mbuf_pool);

  if (rc < 0) {
    std::fprintf(stderr, "rte_eth_rx_queue_setup failed: %s\n", rte_strerror(-rc));
    return -1;
  }

  // rte_eth_dev_start() [rte_ethdev.h]
  // Start the port. After this, rx_burst is "ready".
  rc = rte_eth_dev_start(cfg.port_id);
  if (rc < 0) {
    std::fprintf(stderr, "rte_eth_dev_start failed: %s\n", rte_strerror(-rc));
    return -1;
  }

  // Optional: promiscuous mode to accept all packets.
  // rte_eth_promiscuous_enable() [rte_ethdev.h]
  if (cfg.promisc) rte_eth_promiscuous_enable(cfg.port_id);

  // rte_eth_link_get_nowait() [rte_ethdev.h]
  // Fetch link status and speed without blocking.
  rte_eth_link link{};
  rte_eth_link_get_nowait(cfg.port_id, &link);

  std::printf("Port %u started. Link: %s %u Mbps\n",
              cfg.port_id,
              link.link_status ? "UP" : "DOWN",
              link.link_speed);

  return 0;
}

// ============================================================
// rx_ring_loop(): Producer/consumer pipeline using rte_ring.
// - Create ring
// - Create mbuf pool for RX driver
// - init_port_rx()
// - Launch N worker lcores
// - Run producer on master lcore
// - Wait workers, aggregate results, stop/close port
//
// Key DPDK calls:
// - rte_ring_create()          [rte_ring.h]
// - rte_pktmbuf_pool_create()  [rte_mbuf.h]
// - rte_eal_remote_launch()    [rte_eal.h]
// - rte_eal_wait_lcore()       [rte_eal.h]
// - rte_eth_dev_stop/close()   [rte_ethdev.h]
// ============================================================
static int rx_ring_loop(void* arg) {
  const AppConfig& cfg = *reinterpret_cast<AppConfig*>(arg);

  // rte_ring_create() [rte_ring.h]
  // Creates a lock-free FIFO ring of pointers.
  // Flags:
  //   RING_F_SP_ENQ => single-producer optimization on enqueue side.
  // No SC_DEQ => multi-consumer safe (multiple workers dequeue).
  g_rx_ring = rte_ring_create(
      "RX_RING",
      RX_RING_SIZE,
      rte_socket_id(),
      RING_F_SP_ENQ);

  if (!g_rx_ring) {
    std::fprintf(stderr, "ring create failed: %s\n", rte_strerror(rte_errno));
    return -1;
  }

  // Create RX mempool (the RX driver allocates mbufs from here).
  const int socket_id = rte_socket_id();
  std::string pool_name = "MBUF_POOL_RXRING_" + std::to_string(socket_id);

  std::printf("rx_ring: creating mbuf pool...\n");

  // rte_pktmbuf_pool_create() [rte_mbuf.h]
  rte_mempool* mbuf_pool = rte_pktmbuf_pool_create(
      pool_name.c_str(),
      cfg.mbufs,
      cfg.mbuf_cache,
      0,
      RTE_MBUF_DEFAULT_BUF_SIZE,
      socket_id);

  if (!mbuf_pool) {
    std::fprintf(stderr, "mempool create failed: %s\n", rte_strerror(rte_errno));
    return -1;
  }

  std::printf("rx_ring: init_port_rx()...\n");
  if (init_port_rx(cfg, mbuf_pool) != 0) return -1;

  std::printf("rx_ring: port ready, launching workers...\n");

  // Launch workers (each worker dequeues from ring)
  std::vector<WorkerCtx> ctxs(cfg.workers);
  std::vector<WorkerResult> results(cfg.workers);

  unsigned idx = 0;
  unsigned l = 0;

  // RTE_LCORE_FOREACH_WORKER(l) [rte_lcore.h]
  // Iterates enabled worker lcores (excludes master lcore).
  RTE_LCORE_FOREACH_WORKER(l) {
    if (idx >= cfg.workers) break;
    ctxs[idx].cfg = cfg;
    ctxs[idx].out = &results[idx];
    ctxs[idx].shared_pool = nullptr;

    // rte_eal_remote_launch() [rte_eal.h]
    // Run rx_worker_loop on worker lcore l.
    rte_eal_remote_launch(rx_worker_loop, &ctxs[idx], l);
    idx++;
  }

  if (idx == 0) {
    std::fprintf(stderr, "No worker lcores enabled. Run EAL with more cores, e.g. -l 0-3\n");
    return -1;
  }

  std::printf("rx_ring: starting producer on master...\n");

  // Run producer on master lcore synchronously.
  rx_producer_loop(arg);

  // Wait workers to exit.
  RTE_LCORE_FOREACH_WORKER(l) {
    rte_eal_wait_lcore(l);
  }

  // Aggregate worker stats
  uint64_t total_pkts = 0;
  uint64_t total_iters = 0;
  for (unsigned i = 0; i < idx; ++i) {
    total_pkts += results[i].pkts;
    total_iters += results[i].iters;
  }

  const double mpps = (double)total_pkts / (double)cfg.duration_s / 1e6;

  std::printf("\nRX_RING DONE\n");
  std::printf("workers=%u total_pkts=%" PRIu64 " total_mpps=%.6f total_iters=%" PRIu64 "\n",
              idx, total_pkts, mpps, total_iters);

  // Merge histograms for a global view
  Log2Hist merged{};
  for (unsigned i = 0; i < idx; ++i) merged.merge_from(results[i].hist);

  std::printf("merged: ");
  merged.print_ns();

  if (cfg.latency) {
    for (unsigned i = 0; i < idx; ++i) {
      std::printf("worker %u: ", i);
      results[i].hist.print_ns();
    }
  }

  // rte_eth_dev_stop() / rte_eth_dev_close() [rte_ethdev.h]
  // Stop traffic then free device resources.
  rte_eth_dev_stop(cfg.port_id);
  rte_eth_dev_close(cfg.port_id);

  return 0;
}

// ============================================================
// print_hugepage_meminfo(): helpful debug: show hugepage status.
// DPDK performance depends on hugepages.
// ============================================================
static void print_hugepage_meminfo() {
  FILE* f = std::fopen("/proc/meminfo", "r");
  if (!f) return;

  char line[256];
  while (std::fgets(line, sizeof(line), f)) {
    if (std::strstr(line, "HugePages_") ||
        std::strstr(line, "Hugepagesize") ||
        std::strstr(line, "Hugetlb")) {
      std::printf("%s", line);
    }
  }
  std::fclose(f);
}

// ============================================================
// synth_loop(): launches synth_worker on N worker lcores,
// aggregates results, prints throughput and latency.
//
// Key DPDK calls:
// - rte_lcore_id(), rte_lcore_count(), rte_lcore_is_enabled() [rte_lcore.h]
// - rte_eal_remote_launch(), rte_eal_wait_lcore()             [rte_eal.h]
// - rte_pktmbuf_pool_create()                                 [rte_mbuf.h]
// ============================================================
static int synth_loop(void* arg) {
  const AppConfig& cfg = *reinterpret_cast<AppConfig*>(arg);

  const unsigned master = rte_lcore_id();
  const unsigned avail = rte_lcore_count(); // includes master
  const unsigned max_workers = (avail > 0) ? (avail - 1) : 0;
  const unsigned want = (unsigned)cfg.workers;
  const unsigned use = (want > max_workers) ? max_workers : want;

  if (use == 0) {
    std::fprintf(stderr, "No worker lcores available. Run EAL with more cores, e.g. -l 0-3\n");
    return -1;
  }

  std::printf("=== /proc/meminfo hugepage summary ===\n");
  print_hugepage_meminfo();
  std::printf("=====================================\n");

  std::vector<WorkerResult> results(use);
  std::vector<WorkerCtx> ctxs(use);

  // Choose actual worker lcore ids
  std::vector<unsigned> worker_lcores;
  worker_lcores.reserve(use);

  for (unsigned l = 0; l < RTE_MAX_LCORE && worker_lcores.size() < use; ++l) {
    if (!rte_lcore_is_enabled(l)) continue;
    if (l == master) continue;
    worker_lcores.push_back(l);
  }

  std::printf("synth: master lcore=%u, workers=%u on lcores:", master, use);
  for (auto l : worker_lcores) std::printf(" %u", l);
  std::printf("\n");

  // Optional shared pool across workers (forces contention/cross-core sharing).
  rte_mempool* shared_pool = nullptr;

  if (cfg.pool == AppConfig::PoolMode::SHARED) {
    const int socket_id = rte_socket_id();
    const uint32_t total_mbufs = cfg.mbufs * use; // interpret as per-worker count
    std::string name = "MBUF_POOL_SHARED_" + std::to_string(socket_id);

    shared_pool = rte_pktmbuf_pool_create(
        name.c_str(),
        total_mbufs,
        cfg.mbuf_cache,
        0,
        RTE_MBUF_DEFAULT_BUF_SIZE,
        socket_id);

    if (!shared_pool) {
      std::fprintf(stderr, "shared mempool create failed: %s\n", rte_strerror(rte_errno));
      return -1;
    }
    std::printf("pool=shared total_mbufs=%u\n", total_mbufs);
  } else {
    std::printf("pool=per_worker mbufs_per_worker=%u\n", cfg.mbufs);
  }

  // Launch workers
  for (unsigned i = 0; i < use; ++i) {
    ctxs[i].cfg = cfg;
    ctxs[i].out = &results[i];
    ctxs[i].shared_pool = shared_pool;

    rte_eal_remote_launch(synth_worker, &ctxs[i], worker_lcores[i]);
  }

  // Wait workers
  for (unsigned i = 0; i < use; ++i) {
    rte_eal_wait_lcore(worker_lcores[i]);
  }

  // Aggregate and print
  uint64_t total_pkts = 0, total_iters = 0;
  for (unsigned i = 0; i < use; ++i) {
    total_pkts += results[i].pkts;
    total_iters += results[i].iters;
  }

  const double mpps = (double)total_pkts / (double)cfg.duration_s / 1e6;

  std::printf("\nDONE (synth, %u workers)\n", use);
  std::printf("duration=%us  total_pkts=%" PRIu64 "  total_mpps=%.3f  total_iters=%" PRIu64 "\n",
              cfg.duration_s, total_pkts, mpps, total_iters);

  // Latency printing per worker
  if (cfg.latency) {
    for (unsigned i = 0; i < use; ++i) {
      std::printf("lcore %u: ", worker_lcores[i]);
      results[i].hist.print_ns();
    }
  }

  // Optional JSONL output for plotting or resume “numbers”
  auto pool_str = (cfg.pool == AppConfig::PoolMode::SHARED) ? "shared" : "per_worker";

  uint64_t p50 = 0, p99 = 0, p999 = 0;
  if (cfg.latency && use > 0) {
    uint64_t worst = 0;
    for (unsigned i = 0; i < use; ++i) {
      uint64_t v = results[i].hist.quantile(0.999);
      if (v > worst) worst = v;
    }
    p999 = worst;
    p50 = results[0].hist.quantile(0.50);
    p99 = results[0].hist.quantile(0.99);
  }

  if (!cfg.json_out.empty()) {
    FILE* f = std::fopen(cfg.json_out.c_str(), "a");
    if (f) {
      std::fprintf(f,
        "{\"mode\":\"synth\",\"workers\":%u,\"pool\":\"%s\",\"burst\":%u,"
        "\"mbufs_per_worker\":%u,\"mbuf_cache\":%u,\"work_nops\":%u,\"duration_s\":%u,"
        "\"total_mpps\":%.3f,\"p50_lb_ns\":%" PRIu64 ",\"p99_lb_ns\":%" PRIu64 ",\"p999_lb_ns\":%" PRIu64 "}\n",
        use, pool_str, cfg.burst, cfg.mbufs, cfg.mbuf_cache, cfg.work_nops, cfg.duration_s,
        mpps, p50, p99, p999);
      std::fclose(f);
    }
  }

  return 0;
}

// ============================================================
// rx_loop(): baseline RX-only polling benchmark.
// - configure port
// - poll rx_burst in a loop
// - optional synthetic per-packet work
// - free mbufs immediately
// - report throughput and latency of empty vs non-empty bursts
//
// Key DPDK calls:
// - rte_pktmbuf_pool_create()   [rte_mbuf.h]
// - rte_eth_dev_*() family      [rte_ethdev.h]
// - rte_eth_rx_burst()          [rte_ethdev.h]
// - rte_pktmbuf_free()          [rte_mbuf.h]
// ============================================================
static int rx_loop(void* arg) {
  const AppConfig& cfg = *reinterpret_cast<AppConfig*>(arg);

  constexpr uint16_t kRxQueues = 1;
  constexpr uint16_t kTxQueues = 0;

  Log2Hist hist_empty{}, hist_nonempty{};
  uint64_t burst_idx = 0;
  const uint64_t sample_mask =
      (cfg.sample_mask_pow2 >= 63) ? ~0ULL : ((1ULL << cfg.sample_mask_pow2) - 1ULL);

  // Create a mempool (packet buffers) on this NUMA socket.
  const int socket_id = rte_socket_id();
  std::string pool_name = "MBUF_POOL_" + std::to_string(socket_id);

  // rte_pktmbuf_pool_create() [rte_mbuf.h]
  rte_mempool* mbuf_pool = rte_pktmbuf_pool_create(
      pool_name.c_str(),
      cfg.mbufs,
      cfg.mbuf_cache,
      0,
      RTE_MBUF_DEFAULT_BUF_SIZE,
      socket_id);

  if (!mbuf_pool) {
    std::fprintf(stderr, "rte_pktmbuf_pool_create failed: %s\n", rte_strerror(rte_errno));
    return -1;
  }

  // Print available ports (useful under vdevs like net_af_packet).
  uint16_t n_ports = rte_eth_dev_count_avail();
  std::printf("DPDK ports available: %u\n", n_ports);
  for (uint16_t p = 0; p < n_ports; ++p) {
    rte_eth_dev_info info{};
    rte_eth_dev_info_get(p, &info);
    std::printf("  port %u: driver=%s if_index=%u\n",
                p,
                info.driver_name ? info.driver_name : "(null)",
                info.if_index);
  }

  if (!rte_eth_dev_is_valid_port(cfg.port_id)) {
    std::fprintf(stderr, "Invalid port id: %u (ports available=%u)\n", cfg.port_id, n_ports);
    return -1;
  }

  rte_eth_conf port_conf{};
  port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_NONE;

  int rc = rte_eth_dev_configure(cfg.port_id, kRxQueues, kTxQueues, &port_conf);
  if (rc < 0) {
    std::fprintf(stderr, "rte_eth_dev_configure failed: %s\n", rte_strerror(-rc));
    return -1;
  }

  rc = rte_eth_rx_queue_setup(
      cfg.port_id,
      cfg.rx_queue_id,
      cfg.rxd,
      socket_id,
      nullptr,
      mbuf_pool);

  if (rc < 0) {
    std::fprintf(stderr, "rte_eth_rx_queue_setup failed: %s\n", rte_strerror(-rc));
    return -1;
  }

  rc = rte_eth_dev_start(cfg.port_id);
  if (rc < 0) {
    std::fprintf(stderr, "rte_eth_dev_start failed: %s\n", rte_strerror(-rc));
    return -1;
  }

  if (cfg.promisc) rte_eth_promiscuous_enable(cfg.port_id);

  rte_eth_link link{};
  rte_eth_link_get_nowait(cfg.port_id, &link);
  std::printf("Port %u started. Link: %s %u Mbps\n",
              cfg.port_id,
              link.link_status ? "UP" : "DOWN",
              link.link_speed);

  std::vector<rte_mbuf*> bufs(cfg.burst);

  RxStats s{};
  const uint64_t hz = rte_get_tsc_hz();
  const uint64_t t_start = rte_rdtsc();
  uint64_t t_last = t_start;

  while (!g_stop.load(std::memory_order_relaxed)) {
    const bool do_sample = cfg.latency && ((burst_idx & sample_mask) == 0);
    uint64_t t0 = 0;
    if (do_sample) t0 = rte_rdtsc();

    // Poll RX queue
    const uint16_t n = rte_eth_rx_burst(cfg.port_id, cfg.rx_queue_id, bufs.data(), cfg.burst);
    s.bursts++;

    if (n == 0) {
      s.empty_polls++;
    } else {
      s.pkts += n;

      // Optional synthetic per-packet CPU work
      if (cfg.work_nops) {
        for (uint16_t i = 0; i < n; ++i) do_work_nops(cfg.work_nops);
      }

      // RX-only benchmark: free immediately
      for (uint16_t i = 0; i < n; ++i) rte_pktmbuf_free(bufs[i]);
    }

    if (do_sample) {
      const uint64_t t1 = rte_rdtsc();
      const uint64_t ns = (uint64_t)((__uint128_t)(t1 - t0) * 1000000000ULL / hz);
      if (n == 0) hist_empty.add(ns);
      else        hist_nonempty.add(ns);
    }

    const uint64_t now = rte_rdtsc();
    const double elapsed_s = double(now - t_start) / double(hz);
    if (elapsed_s >= cfg.duration_s) break;

    const double since_last = double(now - t_last) / double(hz);
    if (since_last >= 1.0) {
      const double mpps = (double)s.pkts / elapsed_s / 1e6;
      std::printf("t = %.1fs pkts = %" PRIu64 " mpps = %.3f bursts = %" PRIu64 " empty_polls = %" PRIu64 "\n",
                  elapsed_s, s.pkts, mpps, s.bursts, s.empty_polls);
      t_last = now;
    }

    burst_idx++;
  }

  const uint64_t t_end = rte_rdtsc();
  const double total_s = double(t_end - t_start) / double(hz);
  const double mpps = (double)s.pkts / total_s / 1e6;
  const double empty_poll_rate = (double)s.empty_polls / total_s;

  std::printf("\nDONE\n");
  std::printf("duration = %.3fs pkts = %" PRIu64 " avg_mpps = %.3f bursts = %" PRIu64
              " empty_polls = %" PRIu64 " (%.3f M/s)\n",
              total_s, s.pkts, mpps, s.bursts, s.empty_polls, empty_poll_rate / 1e6);

  if (cfg.latency) {
    std::printf("empty bursts: ");
    hist_empty.print_ns();
    std::printf("non-empty bursts: ");
    hist_nonempty.print_ns();
  }

  rte_eth_dev_stop(cfg.port_id);
  rte_eth_dev_close(cfg.port_id);
  return 0;
}

// ============================================================
// main():
// - install SIGINT handler
// - rte_eal_init() consumes DPDK args and returns number consumed
// - remaining args are app args (starting at argv[0] again)
// - parse_args()
// - dispatch mode
//
// Key DPDK call:
// - rte_eal_init() [rte_eal.h]
// ============================================================
int main(int argc, char** argv) {
  std::signal(SIGINT, on_sigint);

  // rte_eal_init() [rte_eal.h]
  // Initializes DPDK runtime: hugepages, memzones, lcores, drivers/vdevs, etc.
  int eal_rc = rte_eal_init(argc, argv);
  if (eal_rc < 0) {
    std::fprintf(stderr, "rte_eal_init failed\n");
    return 1;
  }

  // After EAL consumes its args, advance argv to app args.
  argc -= eal_rc;
  argv += eal_rc;

  AppConfig cfg;
  if (!parse_args(argc, argv, cfg)) {
    usage("rx_only");
    return 1;
  }

  if (cfg.mode == AppConfig::Mode::RX_RING) {
    return rx_ring_loop(&cfg);
  } else if (cfg.mode == AppConfig::Mode::SYNTH) {
    return synth_loop(&cfg);
  }

  // Default: rx mode
  return rx_loop(&cfg);
}
