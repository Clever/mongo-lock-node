include node.mk
.PHONY: all test build lint
SHELL := /bin/bash

NODE_VERSION := "v18"

TS_FILES := $(shell find . -name "*.ts" -not -path "./node_modules/*" -not -name "*numbro-polyfill.ts" -not -path "./__tests__/*" -not -path "./__mocks__/*")

all: test build

lint:
	@echo "Linting..."
	@./node_modules/.bin/eslint $(TS_FILES)

test: lint
	@echo "Testing..."
	@npm run --silent test

build:
	@echo "Building..."
	@npm run --silent build
