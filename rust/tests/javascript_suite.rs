#![allow(dead_code)]

#[path = "../src/batch.rs"]
mod batch;
#[path = "../src/core.rs"]
mod core;
#[path = "../src/eager_promise.rs"]
mod eager_promise;
#[path = "../src/map.rs"]
mod map;
#[path = "../src/rpc.rs"]
mod rpc;
#[path = "../src/serialize.rs"]
mod serialize;
#[path = "../src/streams.rs"]
mod streams;
#[path = "../src/websocket.rs"]
mod websocket;

#[path = "javascript/flow_control_test.rs"]
mod flow_control_test;
#[path = "javascript/index_test.rs"]
mod index_test;
#[path = "javascript/limits_test.rs"]
mod limits_test;
