#![allow(dead_code, missing_docs)]

#[path = "../examples/kitchen-sink/generated.rs"]
mod bindings;

#[test]
fn generated_kitchen_sink_bindings_compile() {
    let _ = std::mem::size_of::<bindings::KitchenSinkApi>();
    let _ = std::mem::size_of::<bindings::User>();
    let _ = std::mem::size_of::<bindings::ByteValues>();
}
