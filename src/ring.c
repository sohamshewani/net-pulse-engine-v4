#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <errno.h>

#ifdef __STDC_NO_ATOMICS__
#error "C11 atomic execution environment required for net-pulse-engine"
#endif

#define NP_CACHELINE_SIZE 64
#define NP_ARENA_CHUNK_DEFAULT (2ULL * 1024ULL * 1024ULL)

typedef struct __attribute__((aligned(NP_CACHELINE_SIZE))) np_packet {
    uint8_t *payload;
    uint32_t wire_len;
    uint32_t flags;
    uint64_t capture_timestamp_ns;
    uint64_t flow_id;
    uint8_t reserved[24];
} np_packet_t;

_Static_assert(sizeof(np_packet_t) == 64, "np_packet_t must be exactly 64 bytes");

typedef struct np_ring {
    uint32_t capacity;
    uint32_t mask;
    char pad0[NP_CACHELINE_SIZE - 8];

    alignas(NP_CACHELINE_SIZE) atomic_uint_fast64_t head;
    char pad1[NP_CACHELINE_SIZE - sizeof(atomic_uint_fast64_t)];

    alignas(NP_CACHELINE_SIZE) atomic_uint_fast64_t tail;
    char pad2[NP_CACHELINE_SIZE - sizeof(atomic_uint_fast64_t)];

    alignas(NP_CACHELINE_SIZE) atomic_uint_fast64_t dropped_packets;
    char pad3[NP_CACHELINE_SIZE - sizeof(atomic_uint_fast64_t)];

    np_packet_t **buffer;
} np_ring_t;

typedef struct np_arena_chunk {
    struct np_arena_chunk *next;
    size_t capacity;
    alignas(NP_CACHELINE_SIZE) atomic_size_t offset;
    char pad[NP_CACHELINE_SIZE - sizeof(atomic_size_t)];
    uint8_t memory[];
} np_arena_chunk_t;

typedef struct np_arena {
    size_t default_chunk_size;
    alignas(NP_CACHELINE_SIZE) _Atomic(np_arena_chunk_t *) current_chunk;
    char pad[NP_CACHELINE_SIZE - sizeof(void *)];
} np_arena_t;

static inline uint32_t np_next_power_of_two(uint32_t x) {
    if (x == 0) return 1;
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    return x + 1;
}

np_ring_t *np_ring_create(uint32_t requested_capacity) {
    uint32_t cap = np_next_power_of_two(requested_capacity);
    size_t total_size = sizeof(np_ring_t) + (cap * sizeof(np_packet_t *));
    
    void *raw = NULL;
    int ret = posix_memalign(&raw, NP_CACHELINE_SIZE, total_size);
    if (ret != 0 || raw == NULL) {
        return NULL;
    }

    np_ring_t *ring = (np_ring_t *)raw;
    ring->capacity = cap;
    ring->mask = cap - 1;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    atomic_init(&ring->dropped_packets, 0);
    ring->buffer = (np_packet_t **)((uint8_t *)raw + sizeof(np_ring_t));

    memset(ring->buffer, 0, cap * sizeof(np_packet_t *));
    return ring;
}

void np_ring_destroy(np_ring_t *ring) {
    if (ring == NULL) {
        return;
    }
    free(ring);
}

bool np_ring_enqueue(np_ring_t *ring, np_packet_t *packet) {
    uint64_t current_head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t current_tail = atomic_load_explicit(&ring->tail, memory_order_acquire);

    if ((current_head - current_tail) >= ring->capacity) {
        atomic_fetch_add_explicit(&ring->dropped_packets, 1, memory_order_relaxed);
        return false;
    }

    if (!atomic_compare_exchange_weak_explicit(&ring->head,
                                               &current_head,
                                               current_head + 1,
                                               memory_order_release,
                                               memory_order_relaxed)) {
        atomic_fetch_add_explicit(&ring->dropped_packets, 1, memory_order_relaxed);
        return false;
    }

    ring->buffer[current_head & ring->mask] = packet;
    return true;
}

bool np_ring_dequeue(np_ring_t *ring, np_packet_t **packet) {
    uint64_t current_tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t current_head = atomic_load_explicit(&ring->head, memory_order_acquire);

    if (current_tail == current_head) {
        return false;
    }

    *packet = ring->buffer[current_tail & ring->mask];

    atomic_store_explicit(&ring->tail, current_tail + 1, memory_order_release);
    return true;
}

uint64_t np_ring_get_dropped(const np_ring_t *ring) {
    if (ring == NULL) {
        return 0;
    }
    return atomic_load_explicit(&ring->dropped_packets, memory_order_relaxed);
}

static np_arena_chunk_t *np_arena_chunk_create(size_t capacity) {
    size_t alloc_size = sizeof(np_arena_chunk_t) + capacity;
    void *raw = mmap(NULL, alloc_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw == MAP_FAILED) {
        return NULL;
    }

    np_arena_chunk_t *chunk = (np_arena_chunk_t *)raw;
    chunk->next = NULL;
    chunk->capacity = capacity;
    atomic_init(&chunk->offset, 0);
    return chunk;
}

np_arena_t *np_arena_create(size_t default_chunk_size) {
    size_t chunk_size = default_chunk_size > 0 ? default_chunk_size : NP_ARENA_CHUNK_DEFAULT;
    
    void *raw = NULL;
    int ret = posix_memalign(&raw, NP_CACHELINE_SIZE, sizeof(np_arena_t));
    if (ret != 0 || raw == NULL) {
        return NULL;
    }

    np_arena_t *arena = (np_arena_t *)raw;
    arena->default_chunk_size = chunk_size;

    np_arena_chunk_t *initial_chunk = np_arena_chunk_create(chunk_size);
    if (initial_chunk == NULL) {
        free(arena);
        return NULL;
    }

    atomic_init(&arena->current_chunk, initial_chunk);
    return arena;
}

void np_arena_destroy(np_arena_t *arena) {
    if (arena == NULL) {
        return;
    }

    np_arena_chunk_t *chunk = atomic_load_explicit(&arena->current_chunk, memory_order_relaxed);
    while (chunk != NULL) {
        np_arena_chunk_t *next = chunk->next;
        size_t alloc_size = sizeof(np_arena_chunk_t) + chunk->capacity;
        munmap(chunk, alloc_size);
        chunk = next;
    }

    free(arena);
}

void *np_arena_alloc(np_arena_t *arena, size_t size) {
    if (arena == NULL || size == 0) {
        return NULL;
    }

    size_t aligned_size = (size + (NP_CACHELINE_SIZE - 1)) & ~(NP_CACHELINE_SIZE - 1);

    np_arena_chunk_t *chunk = atomic_load_explicit(&arena->current_chunk, memory_order_acquire);

    while (chunk != NULL) {
        size_t current_offset = atomic_load_explicit(&chunk->offset, memory_order_relaxed);
        
        while (current_offset + aligned_size <= chunk->capacity) {
            if (atomic_compare_exchange_weak_explicit(&chunk->offset,
                                                      &current_offset,
                                                      current_offset + aligned_size,
                                                      memory_order_relaxed,
                                                      memory_order_relaxed)) {
                return (void *)&chunk->memory[current_offset];
            }
        }

        np_arena_chunk_t *next_chunk = atomic_load_explicit(&chunk->next, memory_order_acquire);
        if (next_chunk == NULL) {
            size_t new_capacity = arena->default_chunk_size;
            if (aligned_size > new_capacity) {
                new_capacity = aligned_size + NP_CACHELINE_SIZE;
            }

            np_arena_chunk_t *created_chunk = np_arena_chunk_create(new_capacity);
            if (created_chunk == NULL) {
                return NULL;
            }

            np_arena_chunk_t *expected_next = NULL;
            if (atomic_compare_exchange_strong_explicit(&chunk->next,
                                                        &expected_next,
                                                        created_chunk,
                                                        memory_order_release,
                                                        memory_order_relaxed)) {
                next_chunk = created_chunk;
            } else {
                size_t cleanup_size = sizeof(np_arena_chunk_t) + created_chunk->capacity;
                munmap(created_chunk, cleanup_size);
                next_chunk = atomic_load_explicit(&chunk->next, memory_order_acquire);
            }
        }

        atomic_compare_exchange_strong_explicit(&arena->current_chunk,
                                                &chunk,
                                                next_chunk,
                                                memory_order_release,
                                                memory_order_relaxed);
        chunk = next_chunk;
    }

    return NULL;
}

void np_arena_reset(np_arena_t *arena) {
    if (arena == NULL) {
        return;
    }

    np_arena_chunk_t *chunk = atomic_load_explicit(&arena->current_chunk, memory_order_acquire);
    while (chunk != NULL) {
        atomic_store_explicit(&chunk->offset, 0, memory_order_relaxed);
        chunk = chunk->next;
    }
}