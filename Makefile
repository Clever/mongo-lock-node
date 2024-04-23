include node.mk
.PHONY: all test build lint
SHELL := /bin/bash

NODE_VERSION := "v18"

TS_FILES := $(shell find . -name "*.ts" -not -path "./node_modules/*" -not -name "*numbro-polyfill.ts")
FORMATTED_FILES := $(TS_FILES) # Add other file types as you see fit, e.g. JSON files, config files
MODIFIED_FORMATTED_FILES := $(shell git diff --name-only master $(FORMATTED_FILES))
PRETTIER := ./node_modules/.bin/prettier

all: test build

format:
	@echo "Formatting modified files..."
	@$(PRETTIER) --write $(MODIFIED_FORMATTED_FILES)

format-all:
	@echo "Formatting all files..."
	@$(PRETTIER) --write $(FORMATTED_FILES)

lint:
	@echo "Linting..."
	@./node_modules/.bin/eslint $(TS_FILES)

test: lint
	@echo "Testing..."
	@npm run --silent test

build:
	@echo "Building..."
	@npm run --silent build
