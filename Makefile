SHELL := /bin/bash
.DEFAULT_GOAL := help
.DELETE_ON_ERROR:
export TAG

.PHONY: help build build-release run fmt format-check clippy shell-check lint test verify workflow-lint theme-gallery dev-up dev-run dev-reset dev-seed dev-traffic dev-traffic-kafka dev-traffic-nats dev-down dev-logs check-local test-integration test-buf-live release-check package package-deb package-rpm sweep sweep-install

help:
	@printf '%s\n' \
	  'build / build-release  Build the debug / release binary (locked dependencies)' \
	  'run                    Build and open the app with your normal config (no fixtures needed)' \
	  'fmt                    Format Rust sources' \
	  'lint                   Check formatting, Clippy and shell syntax' \
	  'test                   Run tests without Docker' \
	  'verify                 Build, lint and run non-Docker tests' \
	  'workflow-lint          Validate GitHub workflows (requires Go; downloads pinned actionlint)' \
	  'theme-gallery          Regenerate all theme previews from the UI (no database needed)' \
	  'dev-up                 Build and start ready-to-use disposable local databases' \
	  'dev-run                Build and open the prepared demos (start with dev-up)' \
	  'dev-reset              Delete fixture data, recreate services and reseed demos' \
	  'dev-seed               Add demo data to running fixtures without resetting existing data' \
	  'dev-traffic            Produce Kafka, Redpanda and NATS traffic every 15 seconds; Ctrl-C stops all' \
	  'dev-traffic-kafka      Produce Kafka traffic only' \
	  'dev-traffic-nats       Produce one NATS demo.live message every 15 seconds; Ctrl-C stops' \
	  'check-local            Check all local fixtures, including Redpanda Schema Registry' \
	  'dev-logs / dev-down     Inspect / remove the local fixtures and their temporary data' \
	  'sweep                  Delete build artifacts unused for 14 days (cargo keeps none itself)' \
	  'test-integration       Test all fixtures, or one with DATASOURCE=postgres|qdrant|kafka|nats|dynamodb|rabbitmq|tui' \
	  'test-buf-live          Verify public Buf label/commit discovery and decoding (Internet)' \
	  'release-check TAG=v...  Verify the release tag matches Cargo version' \
	  'package TAG=v...        Build a native archive and SHA-256 file under dist/' \
	  'package-deb TAG=v...    Package on Debian/Ubuntu (requires Go; run package first)' \
	  'package-rpm TAG=v...    Package on Fedora (requires Go; run package first)'

build:
	cargo build --workspace --locked

build-release:
	cargo build --release --locked

run: build
	./target/debug/onetui

fmt:
	cargo fmt --all

format-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets --locked -- -D warnings

shell-check:
	for script in hack/dev.sh scripts/release.sh; do bash -n "$$script" || exit; done
	for script in hack/fixtures/*.sh; do sh -n "$$script" || exit; done

lint: format-check clippy shell-check

test:
	cargo test --workspace --locked

verify: build lint test

workflow-lint:
	go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 -shellcheck= .github/workflows/*.yaml

theme-gallery:
	cargo test -p onetui-tui --lib theme_gallery::export --locked -- --ignored --exact

dev-up: build
	bash hack/dev.sh up

dev-run: build
	bash hack/dev.sh run

dev-reset: build
	bash hack/dev.sh down
	bash hack/dev.sh up

dev-seed: build
	bash hack/dev.sh seed

dev-traffic: build
	bash hack/dev.sh traffic

dev-traffic-kafka: build
	bash hack/dev.sh traffic-kafka

dev-traffic-nats: build
	bash hack/dev.sh traffic-nats

dev-down:
	bash hack/dev.sh down

dev-logs:
	bash hack/dev.sh logs

check-local: build
	bash hack/dev.sh check

test-integration: build
	bash hack/dev.sh test "$${DATASOURCE:-all}"

test-buf-live:
	cargo test -p onetui-kafka --lib hosted_buf_label_and_pinned_commit_decode_the_same_message --locked -- --ignored --nocapture

# cargo never garbage-collects target/, so incremental and dep artifacts from
# deleted branches and old dependency versions accumulate without bound. Left
# alone this reaches a size where cargo spends minutes stat-ing files it will
# not use, and a no-op build becomes slower than a full rebuild.
sweep: sweep-install
	cargo sweep --time 14

sweep-install:
	@command -v cargo-sweep >/dev/null || cargo install cargo-sweep --locked

release-check:
	bash scripts/release.sh check "$$TAG"

package:
	bash scripts/release.sh package "$$TAG"

package-deb:
	bash scripts/release.sh deb "$$TAG"

package-rpm:
	bash scripts/release.sh rpm "$$TAG"
