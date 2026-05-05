# DPDK Kernel Bypass Microbenchmark (RX / Synthetic Path)

A low-latency, multi-core **DPDK kernel-bypass microbenchmark** designed to study packet-path behavior, memory pool contention, and per-core scalability under realistic HFT-style constraints.

This project intentionally focuses on **deterministic performance, explicit memory management, NUMA awareness, and cycle-level latency measurement** rather than application-level trading logic.

---

## Why This Project Matters (HFT Context)

This lab demonstrates skills directly relevant to high-frequency trading systems:

- User-space packet processing (kernel bypass via **DPDK**)
- Explicit **hugepage** memory management
- Per-lcore vs shared **mempool contention analysis**
- Multi-core scaling behavior
- Synthetic workload injection to model downstream strategy cost
- **Latency histograms** (p50 / p95 / p99 / p999)
- Reproducible benchmarking with machine-readable output

This is not a toy example — it is a **controlled performance experiment harness**.

---

## Features

### Execution Modes
- **RX mode**
  Polls a NIC RX queue (or `net_pcap`) and measures empty vs non-empty burst latency.

- **Synthetic mode**
  Generates packet-like work entirely in user space to measure:
  - allocator pressure
  - mempool contention
  - instruction-level workload cost

### Memory Pool Strategies
- **per_worker**
  One `rte_mempool` per lcore (max locality, no contention).

- **shared**
  Single shared `rte_mempool` across workers (models allocator contention).

### Latency Measurement
- Cycle-accurate timing via `rte_rdtsc()`
- Log₂ bucketed histograms
- p50 / p95 / p99 / p999 lower-bound reporting

### Synthetic Load Injection
- Optional `--work-nops N` to simulate per-packet strategy cost
- Makes back-pressure and scaling effects visible

### Reproducible Benchmarks
- JSON Lines output (`--json-out`)
- Deterministic test sweeps via scripts

---

## Repository Structure
├── CMakeLists.txt
├── README.md
├── src/
│ └── rx_only.cpp # RX + synthetic benchmark implementation
├── include/
├── bench/
│ └── results.jsonl # Generated benchmark results
├── scripts/
│ ├── env_check.sh # Environment + hugepage sanity check
│ └── sweep.sh # Reproducible benchmark sweep
└── build/ # CMake build output (gitignored)


---

## Build Instructions (Linux / WSL)

### Dependencies
- Linux (tested on WSL2 Ubuntu)
- DPDK (via `libdpdk-dev`)
- Hugepages enabled
- `pkg-config`, `numactl`

```bash
sudo apt update
sudo apt install -y libdpdk-dev pkg-config numactl

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

./build/rx_only
```
## Hugepage Setup (Required)
```bash
sudo sysctl -w vm.nr_hugepages=256
sudo mkdir -p /dev/hugepages
sudo mount -t hugetlbfs none /dev/hugepages
# Verify
grep Huge /proc/meminfo
```

## Example Usage
### Synthetic Mode (Single Worker)
```bash
sudo ./build/rx_only -l 0-1 -n 4 -- \
  --mode synth \
  --workers 1 \
  --pool per_worker \
  --duration 3 \
  --burst 32 \
  --mbufs 8192 \
  --mbuf-cache 256 \
  --latency
```
### Multicore Scaling (5 Workers)
```bash
sudo ./build/rx_only -l 0-5 -n 4 -- \
  --mode synth \
  --workers 5 \
  --pool per_worker \
  --duration 3 \
  --burst 32 \
  --mbufs 8192 \
  --mbuf-cache 256 \
  --latency
```

### Contention Study (Shared Pool + Workload)
```bash
sudo ./build/rx_only -l 0-5 -n 4 -- \
  --mode synth \
  --workers 5 \
  --pool shared \
  --duration 3 \
  --burst 32 \
  --mbufs 8192 \
  --mbuf-cache 256 \
  --work-nops 200 \
  --latency
```

## Reproducible Benchmark Sweep
```bash
scripts/sweep.sh
```
Results appended to
```bash
bench/results.jsonl
```

### Example JSONL Entry
```json
{
  "mode": "synth",
  "workers": 5,
  "pool": "per_worker",
  "burst": 32,
  "mbufs_per_worker": 8192,
  "work_nops": 0,
  "duration_s": 3,
  "total_mpps": 281.804,
  "p50_lb_ns": 256,
  "p99_lb_ns": 256,
  "p999_lb_ns": 512
}
```

### Observed Results
| Workers | Pool       | Work | Throughput (Mpps) | p50 (ns) | p999 (ns) |
| ------: | ---------- | ---- | ----------------: | -------: | --------: |
|       1 | per_worker | 0    |               ~83 |     ~128 |      ~256 |
|       3 | per_worker | 0    |              ~205 |     ~128 |      ~512 |
|       5 | per_worker | 0    |              ~282 |     ~256 |      ~512 |
|       5 | shared     | 0    |              ~274 |     ~256 |     ~1024 |
|       5 | per_worker | 200  |               ~75 |    ~2048 |     ~8192 |
|       5 | shared     | 200  |               ~56 |    ~2048 |    ~16384 |

## Possible Extensions

-AF_XDP comparison path

-NUMA cross-socket experiments

-Lock-free ring between RX → strategy threads

-Hardware timestamping

-Real NIC (mlx5 / ixgbe) tests
