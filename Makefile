BUILD_TYPE ?= Debug
BUILD_DIR = build

.PHONY: all clean rebuild

all: $(BUILD_DIR)
	@git submodule update --init --recursive
	@cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) ..
	@cmake --build $(BUILD_DIR)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

clean:
	@rm -rf $(BUILD_DIR)

rebuild: clean all