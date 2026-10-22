#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <math.h>

#define WARMUP_ITERATIONS 100000
#define MEASURE_ITERATIONS 1000000

// Mock implementation of the net-pulse-engine core operation
typedef struct {
    uint64_t packet_id;
    uint32_t flags;
    uint8_t payload[64];
} npe_context_t;

static inline void npe_engine_pulse(npe_context_t *ctx) {
    // Simulate lightweight packet processing workload
    ctx->packet_id++;
    ctx->flags ^= 0x1;
    // Simple mixing to prevent compiler optimization
    for (int i = 0; i < 8; i++) {
        ctx->payload[i] ^= (uint8_t)(ctx->packet_id >> (i * 8));
    }
}

static uint64_t get_nanos(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t arg1 = *(const uint64_t *)a;
    uint64_t arg2 = *(const uint64_t *)b;
    if (arg1 < arg2) return -1;
    if (arg1 > arg2) return 1;
    return 0;
}

int main(void) {
    npe_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    uint64_t *latencies = malloc(MEASURE_ITERATIONS * sizeof(uint64_t));
    if (!latencies) {
        fprintf(stderr, "Failed to allocate memory for latencies\n");
        return 1;
    }

    // Warmup phase
    for (uint64_t i = 0; i < WARMUP_ITERATIONS; i++) {
        npe_engine_pulse(&ctx);
    }

    // Measurement phase
    uint64_t start_global = get_nanos();

    for (uint64_t i = 0; i < MEASURE_ITERATIONS; i++) {
        uint64_t start_op = get_nanos();
        npe_engine_pulse(&ctx);
        uint64_t end_op = get_nanos();
        latencies[i] = end_op - start_op;
    }

    uint64_t end_global = get_nanos();
    uint64_t total_time_ns = end_global - start_global;

    // Sort latencies for percentile calculation
    qsort(latencies, MEASURE_ITERATIONS, sizeof(uint64_t), compare_uint64);

    uint64_t p50 = latencies[(size_t)(MEASURE_ITERATIONS * 0.50)];
    uint64_t p90 = latencies[(size_t)(MEASURE_ITERATIONS * 0.90)];
    uint64_t p99 = latencies[(size_t)(MEASURE_ITERATIONS * 0.99)];
    uint64_t p999 = latencies[(size_t)(MEASURE_ITERATIONS * 0.999)];
    uint64_t max_lat = latencies[MEASURE_ITERATIONS - 1];

    double seconds = (double)total_time_ns / 1000000000.0;
    double ops_sec = (double)MEASURE_ITERATIONS / seconds;

    // Print formatted tabular output
    printf("\n");
    printf("=================================================================\n");
    printf("              NET-PULSE-ENGINE MICROBENCHMARK REPORT             \n");
    printf("=================================================================\n");
    printf(" Metric                  | Value                         \n");
    printf("-------------------------+-------------------------------\n");
    printf(" Total Iterations        | %-12lu                  \n", (unsigned long)MEASURE_ITERATIONS);
    printf(" Total Duration (s)      | %-12.4f                  \n", seconds);
    printf(" Throughput (ops/sec)    | %-12.2f                  \n", ops_sec);
    printf("-------------------------+-------------------------------\n");
    printf(" Latency Percentiles     | Nanoseconds (ns)              \n");
    printf("-------------------------+-------------------------------\n");
    printf(" p50                     | %-12lu ns               \n", (unsigned long)p50);
    printf(" p90                     | %-12lu ns               \n", (unsigned long)p90);
    printf(" p99                     | %-12lu ns               \n", (unsigned long)p99);
    printf(" p99.9                   | %-12lu ns               \n", (unsigned long)p999);
    printf(" Max                     | %-12lu ns               \n", (unsigned long)max_lat);
    printf("=================================================================\n");
    printf("\n");

    free(latencies);
    return 0;
}