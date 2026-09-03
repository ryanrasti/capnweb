//! Direct structural translation of `src/serialize.ts`.

use std::collections::BTreeMap;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use num_bigint::BigInt;
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as Json, json};

use crate::core::{
    CapnwebValue, PropertyPath, RpcPayload, RpcPromise, RpcStub, StubHook, TargetStubHook,
};
use crate::{Error, Result};

/// Default maximum recursive value depth.
pub const DEFAULT_MAX_DEPTH: usize = 256;

/// Receive-side resource limits corresponding to JS `RpcLimits`.
#[derive(Clone, Debug)]
pub struct RpcLimits {
    /// Maximum characters in a decimal bigint representation.
    pub max_bigint_digits: usize,
    /// Maximum recursive expression depth.
    pub max_depth: usize,
    /// Maximum encoded message size in bytes.
    pub max_message_size: usize,
}

impl Default for RpcLimits {
    fn default() -> Self {
        Self {
            max_bigint_digits: 16_384,
            max_depth: DEFAULT_MAX_DEPTH,
            max_message_size: 32 * 1024 * 1024,
        }
    }
}

/// Import ID from the sender's perspective.
pub type ImportId = i64;
/// Export ID from the sender's perspective.
pub type ExportId = i64;

/// Direct translation of the JS `Exporter` interface.
pub(crate) trait Exporter {
    fn export_stub(&self, hook: StubHook) -> Result<ExportId>;
    fn export_promise(&self, hook: StubHook) -> Result<ExportId>;
    fn get_import(&self, hook: &StubHook) -> Option<ImportId>;
}

/// Direct translation of the JS `Importer` interface.
pub(crate) trait Importer {
    fn import_stub(&self, id: ImportId) -> Result<StubHook>;
    fn import_promise(&self, id: ImportId) -> Result<StubHook>;
    fn get_export(&self, id: ExportId) -> Option<StubHook>;
}

struct NullContext;

impl Exporter for NullContext {
    fn export_stub(&self, _hook: StubHook) -> Result<ExportId> {
        Err(Error::Type(
            "cannot serialize a stub without an RPC session".into(),
        ))
    }

    fn export_promise(&self, _hook: StubHook) -> Result<ExportId> {
        Err(Error::Type(
            "cannot serialize a promise without an RPC session".into(),
        ))
    }

    fn get_import(&self, _hook: &StubHook) -> Option<ImportId> {
        None
    }
}

impl Importer for NullContext {
    fn import_stub(&self, _id: ImportId) -> Result<StubHook> {
        Err(Error::Type(
            "cannot deserialize a stub without an RPC session".into(),
        ))
    }

    fn import_promise(&self, _id: ImportId) -> Result<StubHook> {
        Err(Error::Type(
            "cannot deserialize a promise without an RPC session".into(),
        ))
    }

    fn get_export(&self, _id: ExportId) -> Option<StubHook> {
        None
    }
}

/// Serializes a standalone pass-by-value value using the Cap'n Web JSON encoding.
pub fn serialize(value: &CapnwebValue) -> Result<String> {
    serde_json::to_string(&Devaluator::new(&NullContext).devaluate(value)?)
        .map_err(|error| Error::Protocol(error.to_string()))
}

/// Deserializes a standalone pass-by-value value with default resource limits.
pub fn deserialize(encoded: &str) -> Result<CapnwebValue> {
    deserialize_with_limits(encoded, RpcLimits::default())
}

/// Deserializes a standalone value with explicit resource limits.
pub fn deserialize_with_limits(encoded: &str, limits: RpcLimits) -> Result<CapnwebValue> {
    if encoded.len() > limits.max_message_size {
        return Err(Error::Protocol("message exceeds maximum size".into()));
    }
    let mut deserializer = serde_json::Deserializer::from_str(encoded);
    deserializer.disable_recursion_limit();
    let expression = Json::deserialize(&mut deserializer)
        .map_err(|error| Error::Protocol(format!("invalid JSON: {error}")))?;
    Ok(Evaluator::with_limits(&NullContext, limits)
        .evaluate(expression)?
        .value)
}

/// Direct translation of JS `Devaluator`.
pub(crate) struct Devaluator<'a> {
    exporter: &'a dyn Exporter,
    limits: RpcLimits,
}

impl<'a> Devaluator<'a> {
    /// Constructs a contextual serializer.
    pub fn new(exporter: &'a dyn Exporter) -> Self {
        Self {
            exporter,
            limits: RpcLimits::default(),
        }
    }

    /// Equivalent of `Devaluator.devaluate()`.
    ///
    /// Difference from JS: the v0 value enum already guarantees that the input
    /// is an RPC-supported value, so prototype/type classification is absent.
    pub fn devaluate(&self, value: &CapnwebValue) -> Result<Json> {
        self.devaluate_value(value, 0)
    }

    fn devaluate_value(&self, value: &CapnwebValue, depth: usize) -> Result<Json> {
        if depth > self.limits.max_depth {
            return Err(Error::Protocol(
                "serialization exceeded maximum allowed depth".into(),
            ));
        }
        match value {
            CapnwebValue::Null => Ok(Json::Null),
            CapnwebValue::Bool(value) => Ok(Json::Bool(*value)),
            CapnwebValue::Number(value) if value.is_nan() => Ok(json!(["nan"])),
            CapnwebValue::Number(value) if *value == f64::INFINITY => Ok(json!(["inf"])),
            CapnwebValue::Number(value) if *value == f64::NEG_INFINITY => Ok(json!(["-inf"])),
            CapnwebValue::Number(value) => serde_json::Number::from_f64(*value)
                .map(Json::Number)
                .ok_or_else(|| Error::Protocol("invalid number".into())),
            CapnwebValue::String(value) => Ok(Json::String(value.clone())),
            CapnwebValue::BigInt(value) => Ok(json!(["bigint", value.to_string()])),
            CapnwebValue::Bytes(value) => Ok(json!(["bytes", STANDARD_NO_PAD.encode(value)])),
            CapnwebValue::Date(Some(value)) => Ok(json!(["date", value])),
            CapnwebValue::Date(None) => Ok(json!(["date", null])),
            CapnwebValue::Url(value) => Ok(json!(["url", value])),
            CapnwebValue::Array(values) => Ok(Json::Array(vec![Json::Array(
                values
                    .iter()
                    .map(|value| self.devaluate_value(value, depth + 1))
                    .collect::<Result<Vec<_>>>()?,
            )])),
            CapnwebValue::Object(values) => Ok(Json::Object(
                values
                    .iter()
                    .map(|(key, value)| {
                        self.devaluate_value(value, depth + 1)
                            .map(|value| (key.clone(), value))
                    })
                    .collect::<Result<JsonMap<_, _>>>()?,
            )),
            CapnwebValue::Target(target) => {
                let id = self
                    .exporter
                    .export_stub(TargetStubHook::create(target.clone()))?;
                Ok(json!(["export", id]))
            }
            CapnwebValue::Stub(stub) => self.devaluate_stub(stub),
            CapnwebValue::Promise(promise) => self.devaluate_promise(promise),
        }
    }

    fn devaluate_stub(&self, stub: &RpcStub) -> Result<Json> {
        let hook = stub.current_hook();
        if let Some(id) = self.exporter.get_import(&hook) {
            Ok(json!(["import", id]))
        } else {
            let id = self.exporter.export_stub(hook.dup())?;
            Ok(json!(["export", id]))
        }
    }

    fn devaluate_promise(&self, promise: &RpcPromise) -> Result<Json> {
        let hook = promise.hook();
        if let Some(id) = self.exporter.get_import(&hook) {
            Ok(json!(["pipeline", id]))
        } else {
            let id = self.exporter.export_promise(hook)?;
            Ok(json!(["promise", id]))
        }
    }
}

/// Direct translation of JS `Evaluator`.
pub(crate) struct Evaluator<'a> {
    importer: &'a dyn Importer,
    limits: RpcLimits,
}

impl<'a> Evaluator<'a> {
    /// Constructs an evaluator with explicit receive-side limits.
    pub fn with_limits(importer: &'a dyn Importer, limits: RpcLimits) -> Self {
        Self { importer, limits }
    }

    /// Equivalent of `Evaluator.evaluate()`.
    pub fn evaluate(&self, expression: Json) -> Result<RpcPayload> {
        Ok(RpcPayload::from_app_return(
            self.evaluate_value(expression, 0)?,
        ))
    }

    fn evaluate_value(&self, expression: Json, depth: usize) -> Result<CapnwebValue> {
        if depth >= self.limits.max_depth {
            return Err(Error::Protocol(
                "message exceeded maximum allowed message depth".into(),
            ));
        }
        match expression {
            Json::Null => Ok(CapnwebValue::Null),
            Json::Bool(value) => Ok(CapnwebValue::Bool(value)),
            Json::Number(value) => value
                .as_f64()
                .map(CapnwebValue::Number)
                .ok_or_else(|| Error::Type("number is outside f64 range".into())),
            Json::String(value) => Ok(CapnwebValue::String(value)),
            Json::Object(values) => Ok(CapnwebValue::Object(
                values
                    .into_iter()
                    .map(|(key, value)| {
                        self.evaluate_value(value, depth + 1)
                            .map(|value| (key, value))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?,
            )),
            Json::Array(values) => self.evaluate_array(values, depth),
        }
    }

    fn evaluate_array(&self, values: Vec<Json>, depth: usize) -> Result<CapnwebValue> {
        if values.len() == 1
            && let Some(items) = values[0].as_array()
        {
            return Ok(CapnwebValue::Array(
                items
                    .iter()
                    .cloned()
                    .map(|value| self.evaluate_value(value, depth + 1))
                    .collect::<Result<Vec<_>>>()?,
            ));
        }

        let tag = values
            .first()
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_owned();
        match tag.as_str() {
            "undefined" => Ok(CapnwebValue::Null),
            "bigint" => {
                let value = string_at(&values, 1, "bigint")?;
                if value.len() > self.limits.max_bigint_digits {
                    return Err(Error::Protocol("bigint exceeds maximum length".into()));
                }
                BigInt::from_str(value)
                    .map(CapnwebValue::BigInt)
                    .map_err(|_| Error::Protocol("invalid decimal bigint".into()))
            }
            "bytes" => {
                let value = string_at(&values, 1, "bytes")?;
                match values.get(2) {
                    None => {}
                    Some(Json::String(marker)) if marker == "Uint8Array" => {}
                    Some(Json::String(marker)) => {
                        return Err(Error::Protocol(format!(
                            "unknown bytes type marker: {marker}"
                        )));
                    }
                    Some(_) => {
                        return Err(Error::Protocol("unknown bytes type marker type".into()));
                    }
                }
                STANDARD_NO_PAD
                    .decode(value)
                    .map(CapnwebValue::Bytes)
                    .map_err(|error| Error::Protocol(format!("invalid base64 bytes: {error}")))
            }
            "date" => match values.get(1) {
                Some(Json::Null) => Ok(CapnwebValue::Date(None)),
                Some(Json::Number(value)) => value
                    .as_f64()
                    .map(|value| CapnwebValue::Date(Some(value)))
                    .ok_or_else(|| Error::Protocol("invalid date timestamp".into())),
                _ => Err(Error::Protocol("invalid date expression".into())),
            },
            "url" => Ok(CapnwebValue::Url(string_at(&values, 1, "URL")?.to_owned())),
            "nan" => Ok(CapnwebValue::Number(f64::NAN)),
            "inf" => Ok(CapnwebValue::Number(f64::INFINITY)),
            "-inf" => Ok(CapnwebValue::Number(f64::NEG_INFINITY)),
            "export" => {
                let id = number_at(&values, 1, "export ID")?;
                Ok(CapnwebValue::Stub(RpcStub::from_hook(
                    self.importer.import_stub(id)?,
                )))
            }
            "promise" => {
                let id = number_at(&values, 1, "promise ID")?;
                Ok(CapnwebValue::Promise(RpcPromise::new(
                    self.importer.import_promise(id)?,
                    Vec::new(),
                )))
            }
            "import" | "pipeline" => self.evaluate_reference(&tag, values, depth),
            "error" => {
                let _name = string_at(&values, 1, "error name")?;
                let message = string_at(&values, 2, "error message")?;
                Err(Error::Remote(message.to_owned()))
            }
            other => Err(Error::Protocol(format!("unsupported expression: {other}"))),
        }
    }

    fn evaluate_reference(
        &self,
        tag: &str,
        values: Vec<Json>,
        depth: usize,
    ) -> Result<CapnwebValue> {
        let id = number_at(&values, 1, "import ID")?;
        let hook = self
            .importer
            .get_export(id)
            .ok_or_else(|| Error::Protocol(format!("no such export: {id}")))?;
        let path = values
            .get(2)
            .and_then(Json::as_array)
            .map(|values| decode_path(values))
            .transpose()?
            .unwrap_or_default();

        let result = if let Some(arguments) = values.get(3) {
            let arguments = arguments
                .as_array()
                .ok_or_else(|| Error::Protocol("call arguments are not an array".into()))?
                .iter()
                .cloned()
                .map(|value| self.evaluate_value(value, depth + 1))
                .collect::<Result<Vec<_>>>()?;
            hook.call(
                path,
                RpcPayload::from_app_params(CapnwebValue::Array(arguments)),
            )
        } else if path.is_empty() {
            if tag == "pipeline" {
                hook.get(Vec::new())
            } else {
                hook.dup()
            }
        } else {
            hook.get(path)
        };

        if tag == "pipeline" {
            Ok(CapnwebValue::Promise(RpcPromise::new(result, Vec::new())))
        } else {
            Ok(CapnwebValue::Stub(RpcStub::from_hook(result)))
        }
    }
}

fn string_at<'a>(values: &'a [Json], index: usize, label: &str) -> Result<&'a str> {
    values
        .get(index)
        .and_then(Json::as_str)
        .ok_or_else(|| Error::Protocol(format!("invalid {label}")))
}

fn number_at(values: &[Json], index: usize, label: &str) -> Result<i64> {
    values
        .get(index)
        .and_then(Json::as_i64)
        .ok_or_else(|| Error::Protocol(format!("invalid {label}")))
}

fn decode_path(values: &[Json]) -> Result<PropertyPath> {
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::Protocol("non-string paths are unsupported in v0".into()))
        })
        .collect()
}
