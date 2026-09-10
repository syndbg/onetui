SHELL := /bin/bash
.DEFAULT_GOAL := help
.DELETE_ON_ERROR:
export TAG

.PHONY: help build build-release run fmt lint test verify workflow-lint dev-up dev-seed dev-traffic dev-down dev-logs check-local test-integration release-check package

help:
	@printf '%s\n' \
	  'build / build-release  Build the debug / release binary (locked dependencies)' \
	  'run                    Build and open the local connection picker (start with dev-up)' \
	  'fmt                    Format Rust sources' \
	  'lint                   Check formatting, Clippy and shell syntax' \
	  'test                   Run tests without Docker' \
	  'verify                 Build, lint and run non-Docker tests' \
	  'workflow-lint          Validate GitHub workflows (requires Go; downloads pinned actionlint)' \
	  'dev-up                 Build and start ready-to-use disposable local databases' \
	  'dev-seed               Add demo data to running fixtures without resetting existing data' \
	  'dev-traffic            Produce one Kafka demo_live record every 15 seconds; Ctrl-C stops' \
	  'check-local            Check local PostgreSQL, Qdrant and Kafka fixtures' \
	  'dev-logs / dev-down     Inspect / remove the local fixtures and their temporary data' \
	  'test-integration       Start fresh fixtures, test, then clean up (refuses existing fixtures)' \
	  'release-check TAG=v...  Verify the release tag matches Cargo version' \
	  'package TAG=v...        Build a native archive and SHA-256 file under dist/'

build:
	cargo build --workspace --locked

build-release:
	cargo build --release --locked

run: build
	bash hack/dev.sh run

fmt:
	cargo fmt --all

lint:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --locked -- -D warnings
	for script in hack/dev.sh hack/release.sh; do bash -n "$$script" || exit; done
	for script in hack/fixtures/*.sh; do sh -n "$$script" || exit; done

test:
	cargo test --workspace --locked

verify: build lint test

workflow-lint:
	go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 -shellcheck= .github/workflows/*.yaml

dev-up: build
	bash hack/dev.sh up

dev-seed: build
	bash hack/dev.sh seed

dev-traffic: build
	bash hack/dev.sh traffic

dev-down:
	bash hack/dev.sh down

dev-logs:
	bash hack/dev.sh logs

check-local: build
	bash hack/dev.sh check

test-integration: build
	bash hack/dev.sh test

release-check:
	bash hack/release.sh check "$$TAG"

package:
	bash hack/release.sh package "$$TAG"
