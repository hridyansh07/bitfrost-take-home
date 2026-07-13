# BitFrost Prime — one-command workflows (https://just.systems)
#
#   just            → verify (the single command that reproduces all checks)
#   just fixtures   → regenerate fixtures/ from scratch

# The single verification command: formatting, lints (deny warnings), full test suite.
default: verify

verify: fmt-check lint test

# Full test suite: unit + fixture-driven integration + realtime sequencer harness.
test:
    cargo test --workspace

# Format the workspace in place.
fmt:
    cargo fmt

# Fail if anything is unformatted (what CI / verify runs).
fmt-check:
    cargo fmt --check

# Lint every target with clippy's default rust-wide groups; warnings are errors.
lint:
    cargo clippy --workspace --all-targets -- -D warnings

alias clippy := lint

# Regenerate fixtures/ from scratch (fixed seed; timestamps anchored at generation time).
fixtures:
    cargo run -p types --bin gen_fixtures

# Realtime sequencer demonstration with output; writes out/ordering_algo.json.
realtime:
    BITFROST_OUT="{{justfile_directory()}}/out" cargo test -p ingester --test ordering_realtime -- --nocapture

build:
    cargo build --workspace
