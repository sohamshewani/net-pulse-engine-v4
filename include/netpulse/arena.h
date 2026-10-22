#ifndef NETPULSE_ARENA_H
#define NETPULSE_ARENA_H

#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>

#define NP_CACHE_LINE_SIZE 64
#define NP_ARENA_ALIGNMENT 64
#define NP_ARENA_DEFAULT_CHUNK_SIZE (2 * 1024 * 1024) /* 2MB Huge-page friendly chunk size */

typedef struct np_arena_chunk {
    struct np_arena_chunk* volatile next;
    size_t capacity;
    _Atomic size_t offset;
    char data[];
} np_arena_chunk_t;

typedef struct alignas(NP_CACHE_LINE_SIZE) np_arena_t {
    _Atomic(np_arena_chunk_t*) current_chunk;
    size_t default_chunk_size;
    _Atomic size_t total_allocated;
    _Atomic size_t total_capacity;
} np_arena_t;

static inline size_t np_arena_align_up(size_t size, size_t alignment) {
    return (size + (alignment - 1)) & ~(alignment - 1);
}

static inline np_arena_chunk_t* np_arena_chunk_create(size_t size) {
    size_t total_size = sizeof(np_arena_chunk_t) + size;
    void* ptr = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        return NULL;
    }
    np_arena_chunk_t* chunk = (np_arena_chunk_t*)ptr;
    chunk->next = NULL;
    chunk->capacity = size;
    atomic_init(&chunk->offset, 0);
    return chunk;
}

static inline void np_arena_chunk_destroy(np_arena_chunk_t* chunk) {
    if (!chunk) return;
    size_t total_size = sizeof(np_arena_chunk_t) + chunk->capacity;
    munmap(chunk, total_size);
}

static inline bool np_arena_init(np_arena_t* arena, size_t initial_chunk_size) {
    if (!arena) return false;
    size_t chunk_size = initial_chunk_size ? initial_chunk_size : NP_ARENA_DEFAULT_CHUNK_SIZE;
    chunk_size = np_arena_align_up(chunk_size, NP_ARENA_ALIGNMENT);

    np_arena_chunk_t* first_chunk = np_arena_chunk_create(chunk_size);
    if (!first_chunk) {
        return false;
    }

    atomic_init(&arena->current_chunk, first_chunk);
    arena->default_chunk_size = chunk_size;
    atomic_init(&arena->total_allocated, 0);
    atomic_init(&arena->total_capacity, chunk_size);
    return true;
}

static inline void np_arena_reset(np_arena_t* arena) {
    if (!arena) return;

    np_arena_chunk_t* head = atomic_load_explicit(&arena->current_chunk, memory_order_acquire);
    while (head != NULL) {
        atomic_store_explicit(&head->offset, 0, memory_order_relaxed);
        head = head->next;
    }

    /* Reset total allocated bytes counter while preserving chunk capacity */
    atomic_store_explicit(&arena->total_allocated, 0, memory_order_release);
}

static inline void np_arena_destroy(np_arena_t* arena) {
    if (!arena) return;

    np_arena_chunk_t* current = atomic_load_explicit(&arena->current_chunk, memory_order_acquire);
    while (current != NULL) {
        np_arena_chunk_t* next = current->next;
        np_arena_chunk_destroy(current);
        current = next;
    }

    atomic_store_explicit(&arena->current_chunk, NULL, memory_order_release);
    atomic_store_explicit(&arena->total_allocated, 0, memory_order_release);
    atomic_store_explicit(&arena->total_capacity, 0, memory_order_release);
}

static inline void* np_arena_alloc(np_arena_t* arena, size_t size) {
    if (!arena || size == 0) return NULL;

    size_t aligned_size = np_arena_align_up(size, NP_ARENA_ALIGNMENT);

    while (true) {
        np_arena_chunk_t* chunk = atomic_load_explicit(&arena->current_chunk, memory_order_acquire);
        if (!chunk) return NULL;

        size_t current_offset = atomic_load_explicit(&chunk->offset, memory_order_relaxed);
        
        while (current_offset + aligned_size <= chunk->capacity) {
            if (atomic_compare_exchange_weak_explicit(&chunk->offset,
                                                      &current_offset,
                                                      current_offset + aligned_size,
                                                      memory_order_relaxed,
                                                      memory_order_relaxed)) {
                atomic_fetch_add_explicit(&arena->total_allocated, aligned_size, memory_order_relaxed);
                return (void*)&chunk->data[current_offset];
            }
            /* CAS failed, current_offset is updated with the new value, retry loop */
        }

        /* Chunk is exhausted, check if another thread already allocated a new chunk or allocate one */
        np_arena_chunk_t* next_chunk = chunk->next;
        if (next_chunk == NULL) {
            size_t new_chunk_size = arena->default_chunk_size;
            if (aligned_size > new_chunk_size) {
                new_chunk_size = np_arena_align_up(aligned_size, NP_ARENA_ALIGNMENT);
            }

            np_arena_chunk_t* newly_allocated = np_arena_chunk_create(new_chunk_size);
            if (!newly_allocated) {
                /* Arena exhaustion failure recovery mode triggered */
                return NULL;
            }

            np_arena_chunk_t* expected_next = NULL;
            if (atomic_compare_exchange_strong_explicit(&chunk->next,
                                                        &expected_next,
                                                        newly_allocated,
                                                        memory_order_release,
                                                        memory_order_acquire)) {
                atomic_fetch_add_explicit(&arena->total_capacity, new_chunk_size, memory_order_relaxed);
                next_chunk = newly_allocated;
            } else {
                /* Another thread raced and installed the next chunk, destroy ours */
                np_arena_chunk_destroy(newly_allocated);
                next_chunk = chunk->next;
            }
        }

        if (next_chunk != NULL) {
            atomic_compare_exchange_strong_explicit(&arena->current_chunk,
                                                    &chunk,
                                                    next_chunk,
                                                    memory_order_release,
                                                    memory_order_acquire);
        }
    }
}

#endif /* NETPULSE_ARENA_H */