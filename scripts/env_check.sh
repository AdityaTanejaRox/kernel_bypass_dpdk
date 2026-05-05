#!/usr/bin/env bash
set -euo pipefail

echo "=== SYSTEM ==="
uname -a || true
echo

echo "=== CPU / NUMA ==="
command -v lscpu >/dev/null 2>&1 && lscpu | egrep -i 'Model name|CPU\(s\)|Thread|Core|Socket|NUMA|MHz|Architecture' || true
echo

echo "=== HUGE PAGES (/proc/meminfo) ==="
grep -E 'HugePages_|Hugepagesize|Hugetlb' /proc/meminfo || true
echo

echo "=== HUGETLBFS MOUNTS ==="
mount | grep -i hugetlbfs || true
echo

echo "=== /dev/hugepages ==="
ls -ld /dev/hugepages || true
ls -lh /dev/hugepages || true
echo

echo "=== DPDK ==="
pkg-config --modversion libdpdk 2>/dev/null || echo "pkg-config libdpdk not found"
pkg-config --cflags libdpdk 2>/dev/null || true
pkg-config --libs libdpdk 2>/dev/null || true
echo
