#define _GNU_SOURCE
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ========================================================================= *
 * NET-PULSE-ENGINE IMPLEMENTATION (SUT)                                     *
 * ========================================================================= */

#define NPE_MAX_NODES 1024
#define NPE_MAX_METRICS 64

typedef enum {
    NPE_STATE_DEAD = 0,
    NPE_STATE_PROBING,
    NPE_STATE_ALIVE,
    NPE_STATE_DRAINING
} npe_node_state_t;

typedef struct {
    uint32_t id;
    _Atomic npe_node_state_t state;
    _Atomic uint64_t consecutive_failures;
    _Atomic uint64_t success_count;
    _Atomic uint64_t total_latency_us;
    uint32_t weight;
} npe_node_t;

typedef struct {
    _Atomic uint64_t total_probes;
    _Atomic uint64_t dropped_probes;
    _Atomic uint64_t state_transitions;
} npe_global_metrics_t;

typedef struct {
    npe_node_t nodes[NPE_MAX_NODES];
    _Atomic uint32_t node_count;
    npe_global_metrics_t metrics;
    _Atomic bool active;
    pthread_rwlock_t rwlock;
} npe_engine_t;

static npe_engine_t *npe_create(void) {
    npe_engine_t *engine = calloc(1, sizeof(npe_engine_t));
    if (!engine) return NULL;
    atomic_init(&engine->node_count, 0);
    atomic_init(&engine->metrics.total_probes, 0);
    atomic_init(&engine->metrics.dropped_probes, 0);
    atomic_init(&engine->metrics.state_transitions, 0);
    atomic_init(&engine->active, true);
    pthread_rwlock_init(&engine->rwlock, NULL);
    return engine;
}

static void npe_destroy(npe_engine_t *engine) {
    if (!engine) return;
    atomic_store(&engine->active, false);
    pthread_rwlock_destroy(&engine->rwlock);
    free(engine);
}

static int npe_add_node(npe_engine_t *engine, uint32_t id, uint32_t weight) {
    if (!engine) return -1;
    
    pthread_rwlock_wrlock(&engine->rwlock);
    uint32_t count = atomic_load(&engine->node_count);
    if (count >= NPE_MAX_NODES) {
        pthread_rwlock_unlock(&engine->rwlock);
        return -1;
    }

    /* Check uniqueness */
    for (uint32_t i = 0; i < count; i++) {
        if (engine->nodes[i].id == id) {
            pthread_rwlock_unlock(&engine->rwlock);
            return -2; /* Duplicate */
        }
    }

    npe_node_t *node = &engine->nodes[count];
    node->id = id;
    atomic_init(&node->state, NPE_STATE_DEAD);
    atomic_init(&node->consecutive_failures, 0);
    atomic_init(&node->success_count, 0);
    atomic_init(&node->total_latency_us, 0);
    node->weight = (weight == 0) ? 1 : weight;

    atomic_store(&engine->node_count, count + 1);
    pthread_rwlock_unlock(&engine->rwlock);
    return 0;
}

static npe_node_t *npe_find_node(npe_engine_t *engine, uint32_t id) {
    uint32_t count = atomic_load(&engine->node_count);
    for (uint32_t i = 0; i < count; i++) {
        if (engine->nodes[i].id == id) {
            return &engine->nodes[i];
        }
    }
    return NULL;
}

static int npe_record_probe(npe_engine_t *engine, uint32_t id, bool success, uint32_t latency_us) {
    if (!engine || !atomic_load(&engine->active)) {
        if (engine) atomic_fetch_add(&engine->metrics.dropped_probes, 1);
        return -1;
    }

    pthread_rwlock_rdlock(&engine->rwlock);
    npe_node_t *node = npe_find_node(engine, id);
    if (!node) {
        pthread_rwlock_unlock(&engine->rwlock);
        atomic_fetch_add(&engine->metrics.dropped_probes, 1);
        return -2;
    }

    atomic_fetch_add(&engine->metrics.total_probes, 1);

    npe_node_state_t old_state = atomic_load(&node->state);
    npe_node_state_t new_state = old_state;

    if (success) {
        atomic_fetch_add(&node->success_count, 1);
        atomic_fetch_add(&node->total_latency_us, latency_us);
        atomic_store(&node->consecutive_failures, 0);

        if (old_state == NPE_STATE_DEAD || old_state == NPE_STATE_PROBING) {
            new_state = NPE_STATE_ALIVE;
        }
    } else {
        uint64_t fails = atomic_fetch_add(&node->consecutive_failures, 1) + 1;
        if (old_state == NPE_STATE_ALIVE && fails >= 3) {
            new_state = NPE_STATE_PROBING;
        } else if (old_state == NPE_STATE_PROBING && fails >= 5) {
            new_state = NPE_STATE_DEAD;
        } else if (old_state == NPE_STATE_DEAD) {
            new_state = NPE_STATE_DEAD;
        }
    }

    if (old_state != new_state) {
        /* Attempt atomic transition */
        npe_node_state_t expected = old_state;
        if (atomic_compare_exchange_strong(&node->state, &expected, new_state)) {
            atomic_fetch_add(&engine->metrics.state_transitions, 1);
        }
    }

    pthread_rwlock_unlock(&engine->rwlock);
    return 0;
}

static int npe_drain_node(npe_engine_t *engine, uint32_t id) {
    pthread_rwlock_rdlock(&engine->rwlock);
    npe_node_t *node = npe_find_node(engine, id);
    if (!node) {
        pthread_rwlock_unlock(&engine->rwlock);
        return -1;
    }
    npe_node_state_t expected = atomic_load(&node->state);
    npe_node_state_t target = NPE_STATE_DRAINING;
    
    // Force transition to draining if not dead
    while (expected != NPE_STATE_DEAD) {
        if (atomic_compare_exchange_weak(&node->state, &expected, target)) {
            atomic_fetch_add(&engine->metrics.state_transitions, 1);
            break;
        }
    }
    pthread_rwlock_unlock(&engine->rwlock);
    return 0;
}

/* ========================================================================= *
 * TEST FRAMEWORK & ASSERTIONS                                               *
 * ========================================================================= */

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "\033[31m[FAIL]\033[0m %s:%d - Assertion failed: %s\n", __FILE__, __LINE__, msg); \
        exit(EXIT_FAILURE); \
    } \
} while(0)

#define RUN_TEST(test_func) do { \
    printf("[RUNNING] %s\n", #test_func); \
    test_func(); \
    printf("\033[32m[PASSED]\033[0m %s\n", #test_func); \
} while(0)

static _Atomic uint32_t assertions_passed = 0;

/* ========================================================================= *
 * 1. UNIT & BOUNDARY TESTS                                                  *
 * ========================================================================= */

static void test_unit_lifecycle_and_boundaries(void) {
    npe_engine_t *engine = npe_create();
    TEST_ASSERT(engine != NULL, "Engine creation failed");

    /* Boundary: Add nodes up to NPE_MAX_NODES */
    for (uint32_t i = 0; i < NPE_MAX_NODES; i++) {
        int res = npe_add_node(engine, i, 10);
        TEST_ASSERT(res == 0, "Failed to add node within capacity");
    }

    /* Boundary: Exceed NPE_MAX_NODES */
    int overflow_res = npe_add_node(engine, NPE_MAX_NODES + 1, 10);
    TEST_ASSERT(overflow_res == -1, "Engine allowed exceeding maximum node capacity");

    /* Edge case: Duplicate node ID */
    int dup_res = npe_add_node(engine, 0, 10);
    TEST_ASSERT(dup_res == -2, "Engine allowed duplicate node insertion");

    /* Edge case: Null engine parameters */
    TEST_ASSERT(npe_add_node(NULL, 999, 10) == -1, "Null engine handling failed on add");
    TEST_ASSERT(npe_record_probe(NULL, 0, true, 100) == -1, "Null engine handling failed on probe");

    /* Edge case: Non-existent node probe */
    int non_node_res = npe_record_probe(engine, 99999, true, 100);
    TEST_ASSERT(non_node_res == -2, "Probe recorded on non-existent node without error");

    npe_destroy(engine);
}

static void test_unit_state_transitions(void) {
    npe_engine_t *engine = npe_create();
    TEST_ASSERT(npe_add_node(engine, 42, 5) == 0, "Setup failed");

    npe_node_t *node = npe_find_node(engine, 42);
    TEST_ASSERT(atomic_load(&node->state) == NPE_STATE_DEAD, "Initial state must be DEAD");

    /* Success pulse should transition DEAD -> ALIVE */
    npe_record_probe(engine, 42, true, 150);
    TEST_ASSERT(atomic_load(&node->state) == NPE_STATE_ALIVE, "Success failed to revive DEAD node");
    TEST_ASSERT(atomic_load(&node->success_count) == 1, "Success count incorrect");

    /* 3 consecutive failures should transition ALIVE -> PROBING */
    npe_record_probe(engine, 42, false, 0);
    npe_record_probe(engine, 42, false, 0);
    TEST_ASSERT(atomic_load(&node->state) == NPE_STATE_ALIVE, "Premature state drop");
    
    npe_record_probe(engine, 42, false, 0); /* 3rd failure */
    TEST_ASSERT(atomic_load(&node->state) == NPE_STATE_PROBING, "ALIVE did not transition to PROBING after 3 fails");

    /* 2 more failures (total 5) should transition PROBING -> DEAD */
    npe_record_probe(engine, 42, false, 0);
    npe_record_probe(engine, 42, false, 0); /* 5th failure */
    TEST_ASSERT(atomic_load(&node->state) == NPE_STATE_DEAD, "PROBING did not transition to DEAD after 5