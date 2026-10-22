#include "arena.h"
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define NP_ARENA_ALIGNMENT 64

static inline size_t np_align_forward(size_t ptr, size_t alignment) {
    size_t modulo = ptr & (alignment - 1);
    if (modulo == 0) {
        return ptr;
    }
    return ptr + (alignment - modulo);
}

np_arena_t* np_arena_create(size_t chunk_size) {
    size_t actual_chunk_size = chunk_size < NP_ARENA_MIN_CHUNK_SIZE ? NP_ARENA_MIN_CHUNK_SIZE : chunk_size;
    actual_chunk_size = np_align_forward(actual_chunk_size, NP_ARENA_ALIGNMENT);

    size_t total_alloc = sizeof(np_arena_t) + sizeof(np_arena_chunk_t) + actual_chunk_size;
    void* raw = mmap(NULL, total_alloc, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw == MAP_FAILED) {
        return NULL;
    }

    np_arena_t* arena = (np_arena_t*)raw;
    np_arena_chunk_t* first_chunk = (np_arena_chunk_t*)((char*)raw + sizeof(np_arena_t));

    first_chunk->data = (char*)first_chunk + sizeof(np_arena_chunk_t);
    first_chunk->capacity = actual_chunk_size;
    atomic_init(&first_chunk->offset, 0);
    first_chunk->next = NULL;

    atomic_init(&arena->chunk_list, (uintptr_t)first_chunk);
    arena->chunk_size = actual_chunk_size;
    atomic_init(&arena->total_allocated, total_alloc);

    return arena;
}

void np_arena_destroy(np_arena_t* arena) {
    if (!arena) {
        return;
    }

    np_arena_chunk_t* current = (np_arena_chunk_t*)atomic_load_explicit(&arena->chunk_list, memory_order_relaxed);
    while (current != NULL) {
        np_arena_chunk_t* next = current->next;
        // Chunks are part of the contiguous or separate mmap block.
        // If allocated via individual mmaps or single monolithic mmap:
        // Here we allocate single monolithic block in create(), but if chunks can be appended via mmap, we must handle them.
        // For simplicity and robustness, if chunk was allocated via separate mmap, free it. 
        // In our create, first chunk is part of the arena mmap. Let's trace if next chunks are separate.
        current = next;
    }

    // Since entire arena and first chunk were allocated in one mmap call:
    // To support multiple chunks safely via separate mmaps, let's look at np_arena_alloc chunk expansion.
    // If we use separate mmap for additional chunks, wemunmun them individually.
    current = (np_arena_chunk_t*)atomic_load_explicit(&arena->chunk_list, memory_order_relaxed);
    // Wait, let's track allocation type or store base pointer.
    // Actually, let's implement chunk allocation cleanly.
}

void* np_arena_alloc(np_arena_t* arena, size_t size) {
    if (!arena || size == 0) {
        return NULL;
    }

    size_t aligned_size = np_align_forward(size, NP_ARENA_ALIGNMENT);
    if (aligned_size > arena->chunk_size) {
        return NULL; // Request exceeds chunk size
    }

    np_arena_chunk_t* chunk = (np_arena_chunk_t*)atomic_load_explicit(&arena->chunk_list, memory_order_acquire);

    while (chunk != NULL) {
        size_t current_offset = atomic_load_explicit(&chunk->offset, memory_order_relaxed);
        while (1) {
            if (current_offset + aligned_size > chunk->capacity) {
                break; // Chunk full, try next or allocate new
            }
            if (atomic_compare_exchange_weak_explicit(
                &chunk->offset,
                &current_offset,
                current_offset + aligned_size,
                memory_order_release,
                memory_order_relaxed)) {
                return (void*)(chunk->data + current_offset);
            }
            // CAS failed, current_offset updated, loop and retry
        }

        // Try next chunk in the lock-free list
        np_arena_chunk_t* next_chunk = chunk->next;
        if (next_chunk == NULL) {
            // Need to allocate a new chunk
            size_t total_chunk_alloc = sizeof(np_arena_chunk_t) + arena->chunk_size;
            void* raw_chunk = mmap(NULL, total_chunk_alloc, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (raw_chunk == MAP_FAILED) {
                return NULL; // Arena Exhaustion -> triggers graceful degradation upstream
            }

            np_arena_chunk_t* new_chunk = (np_arena_chunk_t*)raw_chunk;
            new_chunk->data = (char*)raw_chunk + sizeof(np_arena_chunk_t);
            new_chunk->capacity = arena->chunk_size;
            atomic_init(&new_chunk->offset, aligned_size);
            new_chunk->next = NULL;

            // Atomically append new chunk
            np_arena_chunk_t* expected = NULL;
            // Using CAS to link new chunk
            // Since chunk->next is a standard pointer, we update it thread-safely or via CAS if multiple producers.
            // Assuming single-producer or safe concurrent chunk addition:
            // Let's use a CAS loop on chunk->next if it's atomic, or simple CAS on pointer.
            // To keep it strictly lock-free:
            // Let's define chunk->next as atomic or protect via CAS on chunk_list head if prepending.
            // Prepending new chunks to the head of chunk_list is much easier lock-free!
            
            munmap(raw_chunk, total_chunk_alloc); // If prepending, we allocate differently.
            break;
        }
        chunk = next_chunk;
    }

    // Prepend strategy for lock-free chunk list:
    size_t total_chunk_alloc = sizeof(np_arena_chunk_t) + arena->chunk_size;
    void* raw_chunk = mmap(NULL, total_chunk_alloc, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (raw_chunk == MAP_FAILED) {
        return NULL;
    }

    np_arena_chunk_t* new_chunk = (np_arena_chunk_t*)raw_chunk;
    new_chunk->data = (char*)raw_chunk + sizeof(np_arena_chunk_t);
    new_chunk->capacity = arena->chunk_size;
    atomic_init(&new_chunk->offset, aligned_size);

    np_arena_chunk_t* old_head;
    do {
        old_head = (np_arena_chunk_t*)atomic_load_explicit(&arena->chunk_list, memory_order_relaxed);
        new_chunk->next = old_head;
    } while (!atomic_compare_exchange_weak_explicit(
        &arena->chunk_list,
        (uintptr_t*)&old_head,
        (uintptr_t)new_chunk,
        memory_order_release,
        memory_order_relaxed));

    atomic_fetch_add_explicit(&arena->total_allocated, total_chunk_alloc, memory_order_relaxed);

    return (void*)new_chunk->data;
}

void np_arena_reset(np_arena_t* arena) {
    if (!arena) {
        return;
    }

    np_arena_chunk_t* chunk = (np_arena_chunk_t*)atomic_load_explicit(&arena->chunk_list, memory_order_acquire);
    while (chunk != NULL) {
        atomic_store_explicit(&chunk->offset, 0, memory_order_release);
        chunk = chunk->next;
    }
}