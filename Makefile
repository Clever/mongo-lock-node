include node.mk
.PHONY: all test build lint
SHELL := /bin/bash

TS_FILES := $(shell find . -name "*.ts" -not -path "./node_modules/*" -not -name "*numbro-polyfill.ts")

all: test build

lint:
	@echo "Linting..."
	@./node_modules/.bin/tslint $(TS_FILES)
	@./node_modules/.bin/eslint -c .eslintrc.yml $(TS_FILES)

test: lint
	@echo "Testing..."
	@npm run --silent test

build:
	@echo "Building..."
	@npm run --silent build
