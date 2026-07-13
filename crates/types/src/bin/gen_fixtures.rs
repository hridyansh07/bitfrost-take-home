//! Thin wrapper around `types::fixtures::generate` — the same code path the integration
//! harness uses to prove the checked-in fixtures match the generator.

use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fixtures"));
    types::fixtures::generate(&output)
}
