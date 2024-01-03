#![allow(dead_code)]

use jsonrpsee::core::Error;
use jsonrpsee::types::error::{CallError, ErrorCode, ErrorObject};

pub fn not_supported() -> Error {
    Error::Call(CallError::Custom(ErrorObject::borrowed(
        ErrorCode::MethodNotFound.code(),
        &"Not supported",
        None,
    )))
}

pub fn internal_error(msg: impl std::convert::AsRef<str>) -> Error {
    Error::Call(CallError::Custom(ErrorObject::owned(
        ErrorCode::InternalError.code(),
        "Internal error",
        Some(msg.as_ref()),
    )))
}

pub fn invalid_params(param: &str, msg: impl std::convert::AsRef<str>) -> Error {
    let error = &format!("Invalid params: {:}", param);

    Error::Call(CallError::Custom(ErrorObject::owned(
        ErrorCode::InvalidParams.code(),
        error,
        Some(msg.as_ref()),
    )))
}
