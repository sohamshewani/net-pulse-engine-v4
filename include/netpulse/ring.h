#ifndef NETPULSE_RING_H
#define NETPULSE_RING_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <time.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#define NP_CACHE_LINE_SIZE 64
#define NP_ALIGNAS(n) __attribute__((aligned(n)))

typedef enum {
    NP_FLAG_NONE         = 0,
    NP_FLAG_TCP          = (1 << 0),
    NP_FLAG_UDP          = (1 << 1),
    NP_FLAG_IPV4         = (1 << 2),
    NP_FLAG_IPV6         = (1 << 3),
    NP_FLAG_RETRANSMIT   = (1 << 4),
    NP_FLAG_DROPPED      = (1 << 5)
} np_packet_flag_t;

typedef struct NP_ALIGNAS(NP_CACHE_LINE_SIZE) {
    uint8_t *payload;
    uint32_t wire_len;
    uint32_t cap_len;
    uint64_t timestamp_ns;
    uint32_t flow_id;
    uint32_t flags;
    uint64_t sequence_num;
    uint64_t ack_num;
    uint8_t reserved[16];
} np_packet_t;

typedef struct NP_ALIGNAS(NP_CACHE_LINE_SIZE) {
    uint32_t capacity;
    uint32_t mask;
    _Alignas(NP_CACHE_LINE_SIZE) atomic_uint_least64_t head;
    _Alignas(NP_CACHE_LINE_SIZE) atomic_uint_least64_t tail;
    _Alignas(NP_CACHE_LINE_SIZE) atomic_uint_least64_t dropped_packets;
    np_packet_t **buffer;
} np_ring_t;

typedef struct np_arena_chunk {
    struct np_arena_chunk *next;
    size_t capacity;
    _Alignas(NP_CACHE_LINE_SIZE) atomic_size_t used;
    uint8_t data[];
} np_arena_chunk_t;

typedef struct NP_ALIGNAS(NP_CACHE_LINE_SIZE) {
    size_t default_chunk_size;
    _Alignas(NP_CACHE_LINE_SIZE) atomic_uintptr_t current_chunk;
    _Alignas(NP_CACHE_LINE_SIZE) atomic_size_t total_allocated;
} np_arena_t;

typedef struct NP_ALIGNAS(NP_CACHE_LINE_SIZE) {
    uint32_t flow_id;
    uint64_t expected_seq;
    uint64_t last_ack;
    uint32_t window_size;
    bool active;
    uint8_t pad[3];
} np_flow_window_t;

static inline np_ring_t *np_ring_create(uint32_t capacity) {
    if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
        return NULL;
    }

    size_t ring_alloc_size = (sizeof(np_ring_t) + (NP_CACHE_LINE_SIZE - 1)) & ~(NP_CACHE_LINE_SIZE - 1);
    void *raw = mmap(NULL, ring_alloc_size + sizeof(np_packet_t*), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw == MAP_FAILED) {
        return NULL;
    }

    np_ring_t *ring = (np_ring_t *)raw;
    ring->capacity = capacity;
    ring->mask = capacity - 1;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    atomic_init(&ring->dropped_packets, 0);

    size_t ptr_array_size = sizeof(np_packet_t *) * capacity;
    void *buf_raw = mmap(NULL, ptr_array_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (buf_raw == MAP_FAILED) {
        munmap(raw, ring_alloc_size);
        return NULL;
    }

    ring->buffer = (np_packet_t **)buf_raw;
    return ring;
}

static inline void np_ring_destroy(np_ring_t *ring) {
    if (!ring) {
        return;
    }
    if (ring->buffer) {
        munmap(ring->buffer, sizeof(np_packet_t *) * ring->capacity);
    }
    size_t ring_alloc_size = (sizeof(np_ring_t) + (NP_CACHE_LINE_SIZE - 1)) & ~(NP_CACHE_LINE_SIZE - 1);
    munmap(ring, ring_alloc_size);
}

static inline bool np_ring_enqueue(np_ring_t *ring, np_packet_t *packet) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);

    if (head - tail >= ring->capacity) {
        atomic_fetch_add_explicit(&ring->dropped_packets, 1, memory_order_relaxed);
        fprintf(stderr, "[NETPULSE_WARN] Ring buffer overflow: tail=%lu head=%lu capacity=%u. Packet dropped.\n",
                (unsigned long)tail, (unsigned long)head, ring->capacity);
        return false;
    }

    ring->buffer[head & ring->mask] = packet;
    atomic_store_explicit(&ring->head, head + 1, memory_order_release);
    return true;
}

static inline bool np_ring_dequeue(np_ring_t *ring, np_packet_t **packet) {
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);

    if (tail == head) {
        return false;
    }

    *packet = ring->buffer[tail & ring->mask];
    atomic_store_explicit(&ring->tail, tail + 1, memory_order_release);
    return true;
}

static inline np_arena_chunk_t *np_arena_chunk_create(size_t size) {
    size_t total_size = sizeof(np_arena_chunk_t) + size;
    void *raw = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw == MAP_FAILED) {
        return NULL;
    }
    np_arena_chunk_t *chunk = (np_arena_chunk_t *)raw;
    chunk->next = NULL;
    chunk->capacity = size;
    atomic_init(&chunk->used, 0);
    return chunk;
}

static inline np_arena_t *np_arena_create(size_t default_chunk_size) {
    void *raw = mmap(NULL, sizeof(np_arena_t), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw == MAP_FAILED) {
        return NULL;
    }

    np_arena_t *arena = (np_arena_t *)raw;
    arena->default_chunk_size = default_chunk_size;
    atomic_init(&arena->total_allocated, 0);

    np_arena_chunk_t *first_chunk = np_arena_chunk_create(default_chunk_size);
    if (!first_chunk) {
        munmap(raw, sizeof(np_arena_t));
        return NULL;
    }

    atomic_init(&arena->current_chunk, (uintptr_t)first_chunk);
    atomic_store_explicit(&arena->total_allocated, default_chunk_size, memory_order_relaxed);
    return arena;
}

static inline void np_arena_destroy(np_arena_t *arena) {
    if (!arena) {
        return;
    }

    uintptr_t curr_val = atomic_load_explicit(&arena->current_chunk, memory_order_relaxed);
    np_arena_chunk_t *chunk = (np_arena_chunk_t *)curr_val;

    while (chunk) {
        np_arena_chunk_t *next = chunk->next;
        size_t total_size = sizeof(np_arena_chunk_t) + chunk->capacity;
        munmap(chunk, total_size);
        chunk = next;
    }

    munmap(arena, sizeof(np_arena_t));
}

static inline void *np_arena_alloc(np_arena_t *arena, size_t size) {
    size_t aligned_size = (size + (NP_CACHE_LINE_SIZE - 1)) & ~(NP_CACHE_LINE_SIZE - 1);
    
    while (true) {
        uintptr_t curr_val = atomic_load_explicit(&arena->current_chunk, memory_order_acquire);
        np_arena_chunk_t *chunk = (np_arena_chunk_t *)curr_val;

        if (!chunk) {
            return NULL;
        }

        size_t old_used = atomic_load_explicit(&chunk->used, memory_order_relaxed);
        while (old_used + aligned_size <= chunk->capacity) {
            if (atomic_compare_exchange_weak_explicit(&chunk->used, &old_used, old_used + aligned_size,
                                                      memory_order_relaxed, memory_order_relaxed)) {
                return (void *)&chunk->data[old_used];
            }
        }

        size_t new_chunk_size = arena->default_chunk_size;
        if (aligned_size > new_chunk_size) {
            new_chunk_size = aligned_size;
        }

        np_arena_chunk_t *new_chunk = np_arena_chunk_create(new_chunk_size);
        if (!new_chunk) {
            fprintf(stderr, "[NETPULSE_CRITICAL] Arena exhaustion: mmap failed for size %zu. Triggering degradation mode.\n", new_chunk_size);
            return NULL;
        }

        new_chunk->next = chunk;

        if (atomic_compare_exchange_strong_explicit(&arena->current_chunk, &curr_val, (uintptr_t)new_chunk,
                                                      memory_order_release, memory_order_relaxed)) {
            atomic_fetch_add_explicit(&arena->total_allocated, new_chunk_size, memory_order_relaxed);
        } else {
            munmap(new_chunk, sizeof(np_arena_chunk_t) + new_chunk_size);
        }
    }
}

static inline void np_arena_reset(np_arena_t *arena) {
    uintptr_t curr_val = atomic_load_explicit(&arena->current_chunk, memory_order_acquire);
    np_arena_chunk_t *chunk = (np_arena_chunk_t *)curr_val;

    while (chunk) {
        atomic_store_explicit(&chunk->used, 0, memory_order_relaxed);
        chunk = chunk->next;
    }
}

static inline void np_flow_window_init(np_flow_window_t *window, uint32_t flow_id, uint32_t window_size) {
    if (!window) {
        return;
    }
    window->flow_id = flow_id;
    window->expected_seq = 0;
    window->last_ack = 0;
    window->window_size = window_size;
    window->active = true;
    memset(window->pad, 0, sizeof(window->pad));
}

static inline bool np_flow_window_update(np_flow_window_t *window, uint64_t seq, uint64_t ack) {
    if (!window || !window->active) {
        return false;
    }

    if (seq < window->expected_seq) {
        return false;
    }

    window->expected_seq = seq + 1;
    window->last_ack = ack;
    return true;
}

#endif /* NETPULSE_RING_H */