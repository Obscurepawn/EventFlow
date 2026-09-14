BUILD_DIR ?= build
JOBS ?= 4
CLANGXX ?= clang++
CMAKE ?= cmake
.PHONY: all configure test clean compdb check-clangd
all: configure
	$(CMAKE) --build $(BUILD_DIR) --parallel $(JOBS)
configure:
	$(CMAKE) -S . -B $(BUILD_DIR) -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER=$(CLANGXX)

test: all
	ctest --test-dir $(BUILD_DIR) --output-on-failure

clean:
	$(CMAKE) --build $(BUILD_DIR) --target clean

# Bear must observe actual compiler invocations, including unchanged translation units.
compdb: configure
	$(CMAKE) --build $(BUILD_DIR) --target clean
	bear --output $(CURDIR)/compile_commands.json -- $(CMAKE) --build $(BUILD_DIR) --parallel $(JOBS)

check-clangd:
	clangd --check=src/main.cpp --compile-commands-dir=$(CURDIR)


.PHONY: format format-check
format:
	$(CMAKE) -DCLANG_FORMAT=$(CLANG_FORMAT) -P cmake/Format.cmake

format-check:
	$(CMAKE) -DCLANG_FORMAT=$(CLANG_FORMAT) -DFORMAT_CHECK=ON -P cmake/Format.cmake

.PHONY: release
release:
	$(CMAKE) -S . -B build-release -G "Unix Makefiles" -DCMAKE_CXX_COMPILER=$(CLANGXX) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
	$(CMAKE) --build build-release --parallel $(JOBS)
