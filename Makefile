include node.mk
.PHONY: all test build lint
SHELL := /bin/bash

NODE_VERSION := "v18"

TS_FILES := $(shell find . -name "*.ts" -not -path "./node_modules/*" -not -name "*numbro-polyfill.ts")
FORMATTED_FILES := $(TS_FILES) # Add other file types as you see fit, e.g. JSON files, config files
MODIFIED_FORMATTED_FILES := $(shell git diff --name-only master $(FORMATTED_FILES))

ESLINT := ./node_modules/.bin/eslint
PRETTIER := ./node_modules/.bin/prettier
JEST := ./node_modules/.bin/jest
TSC := ./node_modules/.bin/tsc

.PHONY: test run install_deps build all clean

all: test build

test:
	@$(JEST)

clean:
	:

format:
	@echo "Formatting modified files..."
	@$(PRETTIER) --write $(MODIFIED_FORMATTED_FILES)

format-all:
	@echo "Formatting all files..."
	@$(PRETTIER) --write $(FORMATTED_FILES)

format-check:
	@echo "Running format check..."
	@$(PRETTIER) --list-different $(FORMATTED_FILES) || \
		(echo -e "❌ \033[0;31m Prettier found discrepancies in the above files. Run 'make format' to fix.\033[0m" && false)

lint-es:
	@echo "Running eslint..."
	@$(ESLINT) $(TS_FILES)

lint-fix:
	@echo "Running eslint --fix..."
	@$(ESLINT) --fix $(TS_FILES) || \
		(echo "\033[0;31mThe above errors require manual fixing.\033[0m" && true)

lint: format-check lint-es

build:
	@echo "Building..."
	@npm run --silent build

install_deps:
	npm install
