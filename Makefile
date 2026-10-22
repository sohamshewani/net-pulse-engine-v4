CC ?= gcc
CFLAGS ?= -std=c11 -O3 -Wall -Wextra -pedantic -Wshadow -Wformat=2 -Wundef
LDFLAGS ?= -lpthread

TARGET = net-pulse-engine
SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = benchmarks

SRCS := $(wildcard $(SRC_DIR)/*.c)
OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRCS))
DEPS := $(OBJS:.o=.d)

.PHONY: all build test clean benchmark directories

all: directories $(BUILD_DIR)/$(TARGET)

directories:
	@mkdir -p $(BUILD_DIR)

build: all

$(BUILD_DIR)/$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | directories
	$(CC) $(CFLAGS) -I$(INC_DIR) -MMD -MP -c $< -o $@

-include $(DEPS)

test: all
	@if [ -d "$(TEST_DIR)" ] && [ "$$($(wildcard $(TEST_DIR)/*.c))" != "" ]; then \
		echo "Running tests..."; \
		$(CC) $(CFLAGS) -I$(INC_DIR) $(TEST_DIR)/*.c $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(BUILD_DIR)/test_runner $(LDFLAGS); \
		$(BUILD_DIR)/test_runner; \
	else \
		echo "No tests found in '$(TEST_DIR)' directory. Skipping."; \
	fi

benchmark: all
	@if [ -d "$(BENCH_DIR)" ] && [ "$$($(wildcard $(BENCH_DIR)/*.c))" != "" ]; then \
		echo "Running benchmarks..."; \
		$(CC) $(CFLAGS) -I$(INC_DIR) $(BENCH_DIR)/*.c $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(BUILD_DIR)/bench_runner $(LDFLAGS); \
		$(BUILD_DIR)/bench_runner; \
	else \
		echo "No benchmarks found in '$(BENCH_DIR)' directory. Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)
	@echo "Cleaned build artifacts."