//! Rust-only replacement for the two dynamic JavaScript boundaries.
//!
//! `rpc_interface` replaces `RpcStub` proxy method interception, while
//! `rpc_target` replaces `RpcTarget` prototype reflection. Everything behind
//! these macros uses the same hook operations as the JavaScript runtime.
//!
//! Parity status: partial. Generated getters and complete descriptor/type
//! conformance remain; see `rust/PARITY-AUDIT.md`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    FnArg, ImplItem, ItemImpl, ItemTrait, Pat, ReturnType, TraitItem, Type, parse_macro_input,
};

/// Generates a typed stub whose methods emit calls through `StubHook.call()`.
#[proc_macro_attribute]
pub fn rpc_interface(_attribute: TokenStream, item: TokenStream) -> TokenStream {
    let interface = parse_macro_input!(item as ItemTrait);
    let visibility = &interface.vis;
    let interface_name = &interface.ident;
    let stub_name = syn::Ident::new(&format!("{interface_name}Stub"), interface_name.span());
    let mut methods = Vec::new();
    let mut wire_names = Vec::new();

    for item in &interface.items {
        let TraitItem::Fn(method) = item else {
            continue;
        };
        let method_name = &method.sig.ident;
        let wire_name = snake_to_lower_camel(&method_name.to_string());
        wire_names.push(wire_name.clone());
        let mut parameters = Vec::new();
        let mut arguments = Vec::new();
        for input in &method.sig.inputs {
            let FnArg::Typed(argument) = input else {
                return compile_error(input, "RPC interface declarations omit the self receiver");
            };
            let Pat::Ident(pattern) = argument.pat.as_ref() else {
                return compile_error(&argument.pat, "RPC arguments must use identifier patterns");
            };
            let name = &pattern.ident;
            let ty = &argument.ty;
            parameters.push(quote!(#name: #ty));
            arguments.push(quote!(::capnweb::IntoCapnwebValue::into_capnweb_value(#name)?));
        }
        let output = match &method.sig.output {
            ReturnType::Default => quote!(()),
            ReturnType::Type(_, output) => quote!(#output),
        };
        methods.push(quote! {
            pub fn #method_name(
                &self,
                #(#parameters),*
            ) -> ::capnweb::TypedRpcPromise<#output> {
                let arguments = (|| -> ::capnweb::Result<Vec<::capnweb::CapnwebValue>> {
                    Ok(vec![#(#arguments),*])
                })();
                let promise = match arguments {
                    Ok(arguments) => self.inner.call(
                        vec![#wire_name.to_owned()],
                        ::capnweb::RpcPayload::from_app_params(
                            ::capnweb::CapnwebValue::Array(arguments)
                        ),
                    ),
                    Err(error) => ::capnweb::RpcPromise::rejected(error),
                };
                ::capnweb::TypedRpcPromise::new(promise)
            }
        });
    }

    quote! {
        #visibility enum #interface_name {}

        impl ::capnweb::RpcInterfaceDescriptor for #interface_name {
            fn has_member(wire_name: &str) -> bool {
                false #( || wire_name == #wire_names)*
            }
        }

        #[derive(Clone)]
        #visibility struct #stub_name {
            inner: ::capnweb::RpcStub,
        }

        impl #stub_name {
            pub fn new(inner: ::capnweb::RpcStub) -> Self {
                Self { inner }
            }

            #(#methods)*
        }
    }
    .into()
}

/// Exposes an inherent implementation through generated name-based dispatch.
///
/// Difference from JS: this macro performs the member lookup that
/// `TargetStubHook.call()` performs through prototype reflection in JavaScript.
#[proc_macro_attribute]
pub fn rpc_target(attribute: TokenStream, item: TokenStream) -> TokenStream {
    let interface = if attribute.is_empty() {
        None
    } else {
        Some(parse_macro_input!(attribute as Type))
    };
    let implementation = parse_macro_input!(item as ItemImpl);
    if implementation.trait_.is_some() {
        return compile_error(
            &implementation,
            "#[rpc_target] must annotate an inherent impl",
        );
    }

    let self_type = &implementation.self_ty;
    let mut arms = Vec::new();
    let mut getter_arms = Vec::new();
    for item in &implementation.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let method_name = &method.sig.ident;
        let wire_name = snake_to_lower_camel(&method_name.to_string());
        let (decoders, arguments) = match method_arguments(&method.sig.inputs) {
            Ok(parts) => parts,
            Err(error) => return error,
        };
        let convert = convert_result(&method.sig.output);

        let has_parameters = method
            .sig
            .inputs
            .iter()
            .any(|input| matches!(input, FnArg::Typed(_)));

        if method.sig.asyncness.is_some() {
            if !has_parameters {
                let getter_convert = convert.clone();
                getter_arms.push(quote! {
                    #wire_name => Ok(::capnweb::PromiseStubHook::from_future(async move {
                        let __result = self.#method_name().await;
                        #getter_convert
                    }))
                });
            }
            arms.push(quote! {
                #wire_name => Ok(::capnweb::PromiseStubHook::from_future(async move {
                    let delivered = args.deliver_resolve().await?;
                    let ::capnweb::CapnwebValue::Array(values) = delivered else {
                        return Err(::capnweb::Error::Type("arguments are not an array".into()));
                    };
                    let mut __arguments = values.into_iter();
                    #(#decoders)*
                    if __arguments.next().is_some() {
                        return Err(::capnweb::Error::Type(format!(
                            "too many arguments for `{}`", #wire_name
                        )));
                    }
                    let __result = self.#method_name(#(#arguments),*).await;
                    #convert
                }))
            });
        } else {
            if !has_parameters {
                let getter_convert = convert.clone();
                getter_arms.push(quote! {
                    #wire_name => Ok(::capnweb::PromiseStubHook::from_future(async move {
                        let __result = self.#method_name();
                        #getter_convert
                    }))
                });
            }
            arms.push(quote! {
                #wire_name => Ok(::capnweb::PromiseStubHook::from_future(async move {
                    let delivered = args.deliver_resolve().await?;
                    let ::capnweb::CapnwebValue::Array(values) = delivered else {
                        return Err(::capnweb::Error::Type("arguments are not an array".into()));
                    };
                    let mut __arguments = values.into_iter();
                    #(#decoders)*
                    if __arguments.next().is_some() {
                        return Err(::capnweb::Error::Type(format!(
                            "too many arguments for `{}`", #wire_name
                        )));
                    }
                    let __result = self.#method_name(#(#arguments),*);
                    #convert
                }))
            });
        }
    }

    let interface_guard = interface.as_ref().map(|interface| {
        quote! {
            if !<#interface as ::capnweb::RpcInterfaceDescriptor>::has_member(&method) {
                return Err(::capnweb::Error::Type(format!(
                    "No such RPC method: {}", method
                )));
            }
        }
    });

    let getter_interface_guard = interface.as_ref().map(|interface| {
        quote! {
            if !<#interface as ::capnweb::RpcInterfaceDescriptor>::has_member(&property) {
                return Err(::capnweb::Error::Type(format!(
                    "No such RPC property: {}", property
                )));
            }
        }
    });

    quote! {
        #implementation

        impl ::capnweb::RpcTargetInner for #self_type {
            fn call(
                self: ::std::sync::Arc<Self>,
                path: ::capnweb::PropertyPath,
                args: ::capnweb::RpcPayload,
            ) -> ::capnweb::Result<::capnweb::StubHook> {
                if path.len() > 1 {
                    let first = vec![path[0].clone()];
                    let remaining = path[1..].to_vec();
                    let property = ::capnweb::RpcTargetInner::get(
                        ::std::sync::Arc::clone(&self),
                        first,
                    )?;
                    return property.call(remaining, args);
                }
                let method = match path.as_slice() {
                    [::capnweb::PropertyKey::String(method)] => method.clone(),
                    _ => {
                        drop(args);
                        return Err(::capnweb::Error::Type(format!(
                            "No such RPC method path: {:?}", path
                        )));
                    }
                };
                #interface_guard
                match method.as_str() {
                    #(#arms,)*
                    _ => Err(::capnweb::Error::Type(format!(
                        "No such RPC method: {}", method
                    ))),
                }
            }

            fn get(
                self: ::std::sync::Arc<Self>,
                path: ::capnweb::PropertyPath,
            ) -> ::capnweb::Result<::capnweb::StubHook> {
                if path.len() > 1 {
                    let first = vec![path[0].clone()];
                    let remaining = path[1..].to_vec();
                    let property = ::capnweb::RpcTargetInner::get(self, first)?;
                    return property.get(remaining);
                }
                let property = match path.as_slice() {
                    [::capnweb::PropertyKey::String(property)] => property.clone(),
                    _ => {
                        return Err(::capnweb::Error::Type(format!(
                            "No such RPC property path: {:?}", path
                        )));
                    }
                };
                #getter_interface_guard
                match property.as_str() {
                    #(#getter_arms,)*
                    _ => Err(::capnweb::Error::Type(format!(
                        "RPC member is not a readable property: {}", property
                    ))),
                }
            }
        }
    }
    .into()
}

fn method_arguments(
    inputs: &syn::punctuated::Punctuated<FnArg, syn::token::Comma>,
) -> Result<(Vec<proc_macro2::TokenStream>, Vec<syn::Ident>), TokenStream> {
    let mut decoders = Vec::new();
    let mut arguments = Vec::new();
    for input in inputs {
        match input {
            FnArg::Receiver(_) => {}
            FnArg::Typed(argument) => {
                let Pat::Ident(pattern) = argument.pat.as_ref() else {
                    return Err(compile_error(
                        &argument.pat,
                        "RPC arguments must use identifier patterns",
                    ));
                };
                let name = &pattern.ident;
                let ty = &argument.ty;
                decoders.push(quote! {
                    let #name: #ty = ::capnweb::FromCapnwebValue::from_capnweb_value(
                        __arguments.next().ok_or_else(|| ::capnweb::Error::Type(
                            format!("missing argument `{}`", stringify!(#name))
                        ))?
                    )?;
                });
                arguments.push(name.clone());
            }
        }
    }
    Ok((decoders, arguments))
}

fn convert_result(output: &ReturnType) -> proc_macro2::TokenStream {
    if matches!(output, ReturnType::Type(_, ty) if is_result_type(ty)) {
        quote! {
            match __result {
                Ok(value) => {
                    let value = ::capnweb::IntoCapnwebValue::into_capnweb_value(value)?;
                    Ok(::std::sync::Arc::new(::capnweb::PayloadStubHook::new(
                        ::capnweb::RpcPayload::from_app_return(value)
                    )) as ::capnweb::StubHook)
                }
                Err(error) => Err(::capnweb::Error::Message(error.to_string())),
            }
        }
    } else {
        quote! {
            let value = ::capnweb::IntoCapnwebValue::into_capnweb_value(__result)?;
            Ok(::std::sync::Arc::new(::capnweb::PayloadStubHook::new(
                ::capnweb::RpcPayload::from_app_return(value)
            )) as ::capnweb::StubHook)
        }
    }
}

fn compile_error(tokens: impl quote::ToTokens, message: &str) -> TokenStream {
    syn::Error::new_spanned(tokens, message)
        .into_compile_error()
        .into()
}

fn is_result_type(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "Result"))
}

fn snake_to_lower_camel(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut uppercase_next = false;
    for character in name.chars() {
        if character == '_' {
            uppercase_next = true;
        } else if uppercase_next {
            result.extend(character.to_uppercase());
            uppercase_next = false;
        } else {
            result.push(character);
        }
    }
    result
}
