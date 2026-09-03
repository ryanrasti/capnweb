#![allow(missing_docs)]

// These ignored tests are executable backlog entries for generated APIs whose
// schema lowering exists but whose runtime support is incomplete. Each test
// should be replaced with a real generated fixture assertion before unignoring.

#[test]
#[ignore = "generated bigint and void fixture methods are not included yet"]
fn generated_bigint_and_void_round_trip() {
    panic!("implement generated bigint and void round trip");
}

#[test]
#[ignore = "generated array container runtime coverage is pending"]
fn generated_mutable_and_readonly_arrays_round_trip() {
    panic!("implement generated array round trip");
}

#[test]
#[ignore = "tuple conversion traits are not implemented"]
fn generated_finite_and_optional_tuples_round_trip() {
    panic!("implement generated tuple round trip");
}

#[test]
#[ignore = "BTreeMap conversion traits are not implemented"]
fn generated_string_indexed_records_round_trip() {
    panic!("implement generated record round trip");
}

#[test]
#[ignore = "recursive Rust object representation is not finalized"]
fn generated_recursive_objects_round_trip() {
    panic!("implement generated recursive object round trip");
}

#[test]
#[ignore = "promise-valued argument forwarding needs a generated fixture"]
fn generated_promise_arguments_forward_without_local_await() {
    panic!("implement generated promise argument forwarding");
}

#[test]
#[ignore = "dynamic fallback needs a generated fixture"]
fn generated_dynamic_values_round_trip() {
    panic!("implement generated dynamic value round trip");
}

#[test]
#[ignore = "local closure conversion to callable targets is not implemented"]
fn generated_callbacks_accept_local_closures() {
    panic!("implement generated callback closure conversion");
}

#[test]
#[ignore = "received function wrappers do not implement callable syntax yet"]
fn generated_received_functions_are_callable() {
    panic!("implement generated callable wrapper test");
}

#[test]
#[ignore = "typed stream runtime wrappers are not implemented"]
fn generated_readable_and_writable_streams_round_trip() {
    panic!("implement generated stream round trip");
}

#[test]
#[ignore = "special value wrappers are incomplete"]
fn generated_special_values_round_trip() {
    panic!("implement generated special-value round trips");
}

#[test]
#[ignore = "structured remote error mapping is not implemented"]
fn generated_remote_errors_preserve_kind_and_properties() {
    panic!("implement generated remote error mapping test");
}

#[test]
#[ignore = "full kitchen-sink bindings do not compile against the runtime yet"]
fn generated_kitchen_sink_bindings_compile() {
    panic!("compile the generated kitchen-sink bindings");
}

#[test]
#[ignore = "generated JavaScript/Rust interoperability is pending"]
fn generated_javascript_and_rust_peers_interoperate() {
    panic!("implement generated JavaScript/Rust interoperability");
}
