//! Direct structural translation of `src/serialize.ts`.

use std::collections::BTreeMap;

use serde_json::{Map as JsonMap, Value as Json, json};

use crate::core::{
    CapnwebValue, PropertyPath, RpcPayload, RpcPromise, RpcStub, StubHook, TargetStubHook,
};
use crate::{Error, Result};

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

/// Direct translation of JS `Devaluator`.
pub(crate) struct Devaluator<'a> {
    exporter: &'a dyn Exporter,
}

impl<'a> Devaluator<'a> {
    /// Constructs a contextual serializer.
    pub fn new(exporter: &'a dyn Exporter) -> Self {
        Self { exporter }
    }

    /// Equivalent of `Devaluator.devaluate()`.
    ///
    /// Difference from JS: the v0 value enum already guarantees that the input
    /// is an RPC-supported value, so prototype/type classification is absent.
    pub fn devaluate(&self, value: &CapnwebValue) -> Result<Json> {
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
            CapnwebValue::Array(values) => Ok(Json::Array(vec![Json::Array(
                values
                    .iter()
                    .map(|value| self.devaluate(value))
                    .collect::<Result<Vec<_>>>()?,
            )])),
            CapnwebValue::Object(values) => Ok(Json::Object(
                values
                    .iter()
                    .map(|(key, value)| self.devaluate(value).map(|value| (key.clone(), value)))
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
        if let Some(id) = self.exporter.get_import(&stub.hook) {
            Ok(json!(["import", id]))
        } else {
            let id = self.exporter.export_stub(stub.hook.dup())?;
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
}

impl<'a> Evaluator<'a> {
    /// Constructs a contextual evaluator.
    pub fn new(importer: &'a dyn Importer) -> Self {
        Self { importer }
    }

    /// Equivalent of `Evaluator.evaluate()`.
    pub fn evaluate(&self, expression: Json) -> Result<RpcPayload> {
        Ok(RpcPayload::from_app_return(
            self.evaluate_value(expression)?,
        ))
    }

    fn evaluate_value(&self, expression: Json) -> Result<CapnwebValue> {
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
                    .map(|(key, value)| self.evaluate_value(value).map(|value| (key, value)))
                    .collect::<Result<BTreeMap<_, _>>>()?,
            )),
            Json::Array(values) => self.evaluate_array(values),
        }
    }

    fn evaluate_array(&self, values: Vec<Json>) -> Result<CapnwebValue> {
        if values.len() == 1
            && let Some(items) = values[0].as_array()
        {
            return Ok(CapnwebValue::Array(
                items
                    .iter()
                    .cloned()
                    .map(|value| self.evaluate_value(value))
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
            "import" | "pipeline" => self.evaluate_reference(&tag, values),
            "error" => Err(Error::Remote(
                values
                    .get(2)
                    .and_then(Json::as_str)
                    .unwrap_or("remote error")
                    .to_owned(),
            )),
            other => Err(Error::Protocol(format!("unsupported expression: {other}"))),
        }
    }

    fn evaluate_reference(&self, tag: &str, values: Vec<Json>) -> Result<CapnwebValue> {
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
                .map(|value| self.evaluate_value(value))
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
