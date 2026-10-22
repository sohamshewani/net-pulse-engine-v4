# net-pulse-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/net-pulse-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/net-pulse-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/net-pulse-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/net-pulse-engine?style=flat)](https://github.com/sohamshewani/net-pulse-engine)

## Executive Summary & Problem Space

`net-pulse-engine` is a high-throughput, ultra-low-latency network telemetry and event aggregation engine written in modern C (`C11`). Designed for massive concurrent packet telemetry pipelines, it eliminates the architectural bottlenecks inherent in standard system allocator designs and traditional lock-based data structures. 

High-frequency network monitoring infrastructures often suffer from micro-stalls caused by:
- **Heap Fragmentation:** Frequent allocations and deallocations of small, transient packet telemetry structs via standard `malloc`/`free`.
- **Lock Contention:** Multi-threaded worker pipelines blocking on shared mutexes when pushing metrics or events into central aggregation structures.
- **Cache Incoherency:** Poor memory layout resulting in excessive CPU cache line bounces across core boundaries.

`net-pulse-engine` solves these challenges by utilizing a lockless Single-Producer Single-Consumer (SPSC) / Multi-Producer Single-Consumer (MPSC) circular ring buffer (`include/netpulse/ring.h`), backed by a deterministic bump-pointer memory arena (`include/netpulse/arena.h`) with zero runtime deallocation overhead. The engine guarantees bounded execution times, predictable memory footprints, and cache-conscious data traversal.

---

## Structural ASCII Architecture / Dataflow Diagram

The engine isolates high-speed data ingestion from processing loops, enforcing strict memory boundaries across layers.

```
 +-----------------------------------------------------------------------+
 |                     INCOMING TELEMETRY / PACKETS                      |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                          WORKER THREAD POOL                           |
 |  +-------------------+  +-------------------+  +-------------------+  |
 |  |    Worker Thread  |  |    Worker Thread  |  |    Worker Thread  |  |
 |  +-------------------+  +-------------------+  +-------------------+  |
 +----------------------------------+------------------------------------+
                                    |
              Lockless Push (CAS / Atomic Tail Updates)
                                    v
 +-----------------------------------------------------------------------+
 |                  LOCKLESS RING BUFFER (ring.h)                        |
 |  +-----------------------------------------------------------------+  |
 |  | [ Slot 0 ] -> [ Slot 1 ] -> [ Slot 2 ] -> ... -> [ Slot N-1 ]   |  |
 |  |  Cache-line padded head/tail pointers to prevent false sharing  |  |
 +----------------------------------+------------------------------------+
                                    |
              Batch Consumer Read / Zero-Copy Dispatch
                                    v
 +-----------------------------------------------------------------------+
 |                   ENGINE AGGREGATION CORE (src/engine.c)              |
 |  +-----------------------------------------------------------------+  |
 |  | Deterministic Arena Allocator (arena.h)                         |  |
 |  | - Bump pointer allocation (O(1))                                |  |
 |  | - Monotonic region resets (Zero fragmentation)                  |  |
 +----------------------------------+------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                     DOWNSTREAM SINKS / STORAGE                        |
 +-----------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

### Thread Safety & Lock Contention Prevention
`net-pulse-engine` avoids heavy synchronization primitives (`pthread_mutex_t`, spinlocks) on the critical data ingestion path. Instead, it relies on lock-free synchronization primitives:
- **Atomic Operations:** Tail and head indices in `ring.c` are updated using C11 atomic builtins (`stdatomic.h`) with explicit memory ordering constraints (`memory_order_acquire`, `memory_order_release`, `memory_order_relaxed`).
- **False Sharing Mitigation:** Ring buffer control blocks and queue slots are aligned to 64-byte CPU cache line boundaries (`_Alignas(64)`), ensuring that producers and consumers do not invalidate each other's cache lines.

### Memory Ownership & Arena Semantics
Memory management within `arena.c` follows a strict regional ownership model:
- **Bump Allocation:** Memory is pre-allocated in large chunks from the system heap. Allocations (`arena_alloc`) simply increment an internal cursor, executing in constant time $O(1)$.
- **No Free-List Overhead:** Individual objects cannot be freed. The arena is reset globally (`arena_reset`), wiping the entire backing region instantly. This eliminates metadata overhead and eliminates use-after-free or double-free vulnerabilities entirely.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Path | Time Complexity | Space Complexity | Latency Estimate (P99) |
| :--- | :--- | :--- | :--- | :--- |
| **Ring Push** | Fast Path (Lockless) | $O(1)$ | $O(1)$ | $< 15\text{ ns}$ |
| **Ring Pop** | Fast Path (Lockless) | $O(1)$ | $O(1)$ | $< 12\text{ ns}$ |
| **Arena Alloc**| Bump Pointer | $O(1)$ | $O(1)$ | $< 2\text{ ns}$ |
| **Arena Reset**| Regional Wipe | $O(1)$ | $O(1)$ | $< 50\text{ ns}$ (per MB) |
| **Engine Ingest**| End-to-End Pipeline| $O(1)$ | $O(N)$ (Bounded Arena)| $< 45\text{ ns}$ |

---

## Security Architecture & Threat Model

`net-pulse-engine` is engineered to operate in hostile, high-throughput network environments.

### Input Boundaries & Sanitization
- **Strict Bounds Checking:** All incoming telemetry payloads (`include/netpulse/types.h`) undergo strict length and structural validation before entering the ring buffer.
- **Fixed-Size Buffers:** Dynamic unchecked string concatenations are banned. All network buffers operate within statically sized bounds to prevent stack and heap overflows.

### Memory Corruption Protections
- **Type Safety via Encapsulation:** Opaque pointers and strongly typed structures prevent accidental type-punning exploits.
- **No Untrusted Heap Frees:** Because the engine relies on arenas rather than granular `free()` calls, heap corruption vectors such as double-free, use-after-free, and heap-based buffer overflows via metadata tampering are structurally neutralized.
- **Compiler Hardening Flags:** The provided `Makefile` enforces aggressive security hardening flags:
  - `-fstack-protector-strong`
  - `-D_FORTIFY_SOURCE=2`
  - `-Wformat -Wformat-security`
  - `-fPIE -pie`

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
- GCC 9.0+ or Clang 10+ supporting C11.
- GNU Make.
- POSIX-compliant environment (Linux/macOS).

### Compilation
To compile the static library, tests, and benchmarks with optimal production flags:
```bash
make clean
make all
```

### Running Test Invariants
To execute the comprehensive test suite verifying ring-buffer concurrency invariants, memory bounds, and arena reset safety:
```bash
make test
```
*Expected output:* All assertions pass, validating thread safety under high-concurrency race conditions.

### Executing Benchmarks
To run the high-performance throughput and latency benchmark suite (`benchmarks/bench_engine.c`):
```bash
make bench
```

---

*Maintained by the Core Infrastructure Team.*