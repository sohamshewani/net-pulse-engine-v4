#ifndef NETPULSE_TYPES_H
#define NETPULSE_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>
#include <sys/types.h>

#define NP_CACHE_LINE_SIZE 64
#define NP_ALIGN(x) __attribute__((aligned(x)))
#define NP_PACKED __attribute__((packed))

typedef enum {
    NP_SUCCESS = 0,
    NP_ERR_NOMEM = -1,
    NP_ERR_FULL = -2,
    NP_ERR_EMPTY = -3,
    NP_ERR_INVAL = -4,
    NP_ERR_OVERFLOW = -5
} np_status_t;

typedef enum {
    NP_FLAG_NONE = 0,
    NP_FLAG_SYN = (1 << 0),
    NP_FLAG_ACK = (1 << 1),
    NP_FLAG_FIN = (1 << 2),
    NP_FLAG_RST = (1 << 3),
    NP_FLAG_RETRANSMIT = (1 << 4),
    NP_FLAG_DROPPED = (1 << 5)
} np_flow_flags_t;

typedef struct NP_ALIGN(NP_CACHE_LINE_SIZE) {
    const uint8_t *payload;
    uint32_t wire_len;
    uint32_t cap_len;
    uint64_t timestamp_ns;
    uint32_t flow_id;
    uint32_t seq;
    uint32_t ack_seq;
    uint16_t flags;
    uint16_t reserved;
    uint8_t padding[16];
} np_packet_t;

_Static_assert(sizeof(np_packet_t) == 64, "np_packet_t must be exactly 64 bytes");

typedef struct NP_ALIGN(NP_CACHE_LINE_SIZE) {
    char pad0[NP_CACHE_LINE_SIZE];
    _Atomic uint64_t head;
    char pad1[NP_CACHE_LINE_SIZE];
    _Atomic uint64_t tail;
    char pad2[NP_CACHE_LINE_SIZE];
    uint32_t capacity;
    uint32_t mask;
    np_packet_t *buffer;
} np_ring_t;

typedef struct np_arena_chunk {
    struct np_arena_chunk *next;
    size_t capacity;
    _Atomic size_t used;
    NP_ALIGN(NP_CACHE_LINE_SIZE) uint8_t data[];
} np_arena_chunk_t;

typedef struct NP_ALIGN(NP_CACHE_LINE_SIZE) {
    np_arena_chunk_t *current_chunk;
    size_t default_chunk_size;
    _Atomic uint64_t total_allocated;
    _Atomic uint64_t total_requested;
} np_arena_t;

typedef struct NP_ALIGN(NP_CACHE_LINE_SIZE) {
    uint32_t flow_id;
    uint32_t expected_seq;
    uint32_t last_ack;
    uint32_t window_size;
    uint64_t last_seen_ns;
    _Atomic uint32_t retransmit_count;
    uint16_t state_flags;
    uint8_t padding[38];
} np_flow_window_t;

_Static_assert(sizeof(np_flow_window_t) == 64, "np_flow_window_t must be exactly 64 bytes");

static inline void np_ring_init(np_ring_t *ring, np_packet_t *storage, uint32_t capacity) {
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    ring->capacity = capacity;
    ring->mask = capacity - 1;
    ring->buffer = storage;
}

static inline bool np_ring_is_full(const np_ring_t *ring) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    return (head - tail) >= ring->capacity;
}

static inline bool np_ring_is_empty(const np_ring_t *ring) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    return head == tail;
}

static inline np_status_t np_ring_enqueue(np_ring_t *ring, const np_packet_t *packet) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);

    if ((head - tail) >= ring->capacity) {
        return NP_ERR_FULL;
    }

    ring->buffer[head & ring->mask] = *packet;
    atomic_store_explicit(&ring->head, head + 1, memory_order_release);
    return NP_SUCCESS;
}

static inline np_status_t np_ring_dequeue(np_ring_t *ring, np_packet_t *packet) {
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_acquire);

    if (head == tail) {
        return NP_ERR_EMPTY;
    }

    *packet = ring->buffer[tail & ring->mask];
    atomic_store_explicit(&ring->tail, tail + 1, memory_order_release);
    return NP_SUCCESS;
}

np_status_t np_arena_init(np_arena_t *arena, size_t default_chunk_size);
void np_arena_destroy(np_arena_t *arena);
void *np_arena_alloc(np_arena_t *arena, size_t size);
void np_arena_reset(np_arena_t *arena);

void np_flow_window_init(np_flow_window_t *window, uint32_t flow_id, uint32_t initial_seq);
bool np_flow_window_update(np_flow_window_t *window, uint32_t seq, uint32_t ack, uint16_t flags);

#endif /* NETPULSE_TYPES_H */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/mman.h>
#include <unistd.h>
#include <string.h>

np_status_t np_arena_init(np_arena_t *arena, size_t default_chunk_size) {
    if (!arena || default_chunk_size == 0) {
        return NP_ERR_INVAL;
    }

    size_t aligned_size = (default_chunk_size + NP_CACHE_LINE_SIZE - 1) & ~(NP_CACHE_LINE_SIZE - 1);
    size_t total_size = sizeof(np_arena_chunk_t) + aligned_size;

    void *raw = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw == MAP_FAILED) {
        return NP_ERR_NOMEM;
    }

    np_arena_chunk_t *chunk = (np_arena_chunk_t *)raw;
    chunk->next = NULL;
    chunk->capacity = aligned_size;
    atomic_init(&chunk->used, 0);

    arena->current_chunk = chunk;
    arena->default_chunk_size = aligned_size;
    atomic_init(&arena->total_allocated, total_size);
    atomic_init(&arena->total_requested, 0);

    return NP_SUCCESS;
}

void np_arena_destroy(np_arena_t *arena) {
    if (!arena) {
        return;
    }

    np_arena_chunk_t *current = arena->current_chunk;
    while (current) {
        np_arena_chunk_t *next = current->next;
        size_t total_size = sizeof(np_arena_chunk_t) + current->capacity;
        munmap(current, total_size);
        current = next;
    }

    arena->current_chunk = NULL;
    atomic_store_explicit(&arena->total_allocated, 0, memory_order_relaxed);
    atomic_store_explicit(&arena->total_requested, 0, memory_order_relaxed);
}

void *np_arena_alloc(np_arena_t *arena, size_t size) {
    if (!arena || size == 0) {
        return NULL;
    }

    size_t aligned_size = (size + NP_CACHE_LINE_SIZE - 1) & ~(NP_CACHE_LINE_SIZE - 1);
    atomic_fetch_add_explicit(&arena->total_requested, aligned_size, memory_order_relaxed);

    np_arena_chunk_t *chunk = arena->current_chunk;
    size_t old_used, new_used;

    while (1) {
        old_used = atomic_load_explicit(&chunk->used, memory_order_relaxed);
        new_used = old_used + aligned_size;

        if (new_used <= chunk->capacity) {
            if (atomic_compare_exchange_weak_explicit(&chunk->used, &old_used, new_used,
                                                     memory_order_relaxed, memory_order_relaxed)) {
                return (void *)&chunk->data[old_used];
            }
        } else {
            size_t alloc_size = (arena->default_chunk_size > aligned_size) ? 
                                arena->default_chunk_size : aligned_size;
            size_t total_size = sizeof(np_arena_chunk_t) + alloc_size;

            void *raw = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (raw == MAP_FAILED) {
                return NULL;
            }

            np_arena_chunk_t *new_chunk = (np_arena_chunk_t *)raw;
            new_chunk->next = NULL;
            new_chunk->capacity = alloc_size;
            atomic_init(&new_chunk->used, aligned_size);

            np_arena_chunk_t *expected_chunk = chunk;
            if (atomic_compare_exchange_strong_explicit(&arena->current_chunk, &expected_chunk, new_chunk,
                                                      memory_order_release, memory_order_relaxed)) {
                new_chunk->next = expected_chunk;
                atomic_fetch_add_explicit(&arena->total_allocated, total_size, memory_order_relaxed);
                return (void *)&new_chunk->data[0];
            } else {
                munmap(raw, total_size);
                chunk = arena->current_chunk;
            }
        }
    }
}

void np_arena_reset(np_arena_t *arena) {
    if (!arena) {
        return;
    }

    np_arena_chunk_t *chunk = arena->current_chunk;
    if (!chunk) {
        return;
    }

    np_arena_chunk_t *head = chunk;
    np_arena_chunk_t *first_chunk = NULL;

    while (chunk) {
        atomic_store_explicit(&chunk->used, 0, memory_order_relaxed);
        first_chunk = chunk;
        chunk = chunk->next;
    }

    if (first_chunk != head) {
        first_chunk->next = head->next;
        head->next = NULL;
        arena->current_chunk = head;
    }

    atomic_store_explicit(&arena->total_requested, 0, memory_order_relaxed);
}

void np_flow_window_init(np_flow_window_t *window, uint32_t flow_id, uint32_t initial_seq) {
    if (!window) {
        return;
    }

    window->flow_id = flow_id;
    window->expected_seq = initial_seq;
    window->last_ack = 0;
    window->window_size = 65535;
    window->last_seen_ns = 0;
    atomic_init(&window->retransmit_count, 0);
    window->state_flags = NP_FLAG_NONE;
    memset(window->padding, 0, sizeof(window->padding));
}

bool np_flow_window_update(np_flow_window_t *window, uint32_t seq, uint32_t ack, uint16_t flags) {
    if (!window) {
        return false;
    }

    if (seq < window->expected_seq) {
        atomic_fetch_add_explicit(&window->retransmit_count, 1, memory_order_relaxed);
        window->state_flags |= NP_FLAG_RETRANSMIT;
        return false;
    }

    window->expected_seq = seq + 1;
    window->last_ack = ack;
    window->state_flags = flags;
    return true;
}