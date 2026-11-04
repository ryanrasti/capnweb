use std::collections::BTreeMap;
use std::sync::Arc;

use capnweb::{CapnwebValue, Error, FromCapnwebValue, IntoCapnwebValue, rpc_target};

#[derive(Debug, PartialEq, IntoCapnwebValue, FromCapnwebValue)]
struct Options {
    user_id: f64,
    working_directory: Option<String>,
    #[capnweb(rename = "env")]
    environment: Vec<String>,
    limits: Option<Limits>,
    labels: BTreeMap<String, bool>,
}

#[derive(Debug, PartialEq, IntoCapnwebValue, FromCapnwebValue)]
struct Limits {
    max_bytes: f64,
}

struct Process;

#[rpc_target]
impl Process {
    fn pid(&self) -> f64 {
        7.0
    }
}

#[derive(IntoCapnwebValue)]
struct Spawned {
    pid: f64,
    process: Arc<Process>,
}

fn object(members: Vec<(&str, CapnwebValue)>) -> CapnwebValue {
    CapnwebValue::Object(
        members
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
}

fn options() -> Options {
    Options {
        user_id: 1000.0,
        working_directory: Some("/tmp".to_owned()),
        environment: vec!["A=1".to_owned()],
        limits: Some(Limits { max_bytes: 64.0 }),
        labels: BTreeMap::from([("fast".to_owned(), true)]),
    }
}

fn type_error(result: capnweb::Result<impl std::fmt::Debug>) -> String {
    match result {
        Err(Error::Type(message)) => message,
        other => panic!("expected a type error, got {other:?}"),
    }
}

#[test]
fn derived_struct_members_are_lower_camel_case_unless_renamed() {
    let CapnwebValue::Object(members) = options().into_capnweb_value().unwrap() else {
        panic!("expected an object");
    };
    assert_eq!(
        members.keys().map(String::as_str).collect::<Vec<_>>(),
        ["env", "labels", "limits", "userId", "workingDirectory"]
    );
    assert!(
        matches!(&members["limits"], CapnwebValue::Object(limits) if limits.contains_key("maxBytes"))
    );
}

#[test]
fn derived_struct_round_trips_through_its_object() {
    let value = options().into_capnweb_value().unwrap();
    assert_eq!(Options::from_capnweb_value(value).unwrap(), options());
}

#[test]
fn none_fields_are_left_out_of_the_object() {
    let value = Options {
        working_directory: None,
        limits: None,
        ..options()
    }
    .into_capnweb_value()
    .unwrap();
    let CapnwebValue::Object(members) = value else {
        panic!("expected an object");
    };
    assert!(!members.contains_key("workingDirectory"));
    assert!(!members.contains_key("limits"));
}

#[test]
fn option_fields_decode_absent_undefined_and_null_as_none() {
    for working_directory in [
        None,
        Some(CapnwebValue::Undefined),
        Some(CapnwebValue::Null),
    ] {
        let mut members = vec![
            ("userId", CapnwebValue::Number(0.0)),
            ("env", CapnwebValue::Array(Vec::new())),
            ("labels", CapnwebValue::Object(BTreeMap::new())),
        ];
        if let Some(value) = working_directory {
            members.push(("workingDirectory", value));
        }
        let decoded = Options::from_capnweb_value(object(members)).unwrap();
        assert_eq!(decoded.working_directory, None);
        assert_eq!(decoded.limits, None);
    }
}

#[test]
fn unknown_members_are_ignored() {
    let CapnwebValue::Object(mut members) = options().into_capnweb_value().unwrap() else {
        panic!("expected an object");
    };
    members.insert("extra".to_owned(), CapnwebValue::Bool(true));
    assert_eq!(
        Options::from_capnweb_value(CapnwebValue::Object(members)).unwrap(),
        options()
    );
}

#[test]
fn decode_errors_name_the_struct_and_member() {
    let missing = object(vec![
        ("env", CapnwebValue::Array(Vec::new())),
        ("labels", CapnwebValue::Object(BTreeMap::new())),
    ]);
    let message = type_error(Options::from_capnweb_value(missing));
    assert!(
        message.starts_with("Options.userId: expected number"),
        "{message}"
    );

    let nested = object(vec![
        ("userId", CapnwebValue::Number(0.0)),
        ("env", CapnwebValue::Array(Vec::new())),
        ("labels", CapnwebValue::Object(BTreeMap::new())),
        (
            "limits",
            object(vec![("maxBytes", CapnwebValue::String("64".to_owned()))]),
        ),
    ]);
    let message = type_error(Options::from_capnweb_value(nested));
    assert!(
        message.starts_with("Options.limits: Limits.maxBytes: expected number"),
        "{message}"
    );

    let message = type_error(Options::from_capnweb_value(CapnwebValue::Number(1.0)));
    assert!(message.starts_with("Options: expected object"), "{message}");
}

#[test]
fn target_fields_become_targets() {
    let value = Spawned {
        pid: 7.0,
        process: Arc::new(Process),
    }
    .into_capnweb_value()
    .unwrap();
    let CapnwebValue::Object(members) = value else {
        panic!("expected an object");
    };
    assert!(matches!(members["pid"], CapnwebValue::Number(pid) if pid == 7.0));
    assert!(matches!(members["process"], CapnwebValue::Target(_)));
}
