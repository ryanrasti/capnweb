//! An idiomatic Rust implementation of the Cap'n Web RPC protocol.
//!
//! The implementation is currently being designed. See `rust-port.md` in this
//! crate's root for the design proposal.

/// The crate version supplied by Cargo.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_is_set() {
        assert!(!VERSION.is_empty());
    }
}
