//! Every refusal of the Platform receipt route as a complete `ErrorEnvelope`.
//!
//! Edge replaces any non-2xx answer that is not `application/json` carrying an `ErrorEnvelope`
//! with `edge.upstream_invalid_response`, so a bare status code erases the reason a receiver
//! refused. The table is built once from the contract grammars and never carries a diagnostic:
//! the boundary logs the failure class once and the envelope stays generic.

use std::sync::OnceLock;

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use ratatoskr_error_contracts::{ErrorCode, ErrorEnvelope};
use ratatoskr_identifiers::SafeMessage;
use uuid::Uuid;

/// The closed set of ways the receipt route refuses a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Claims are absent or unknown, or an `Authorization` header is present.
    Unauthenticated,
    /// The Platform user is authenticated but is not mapped to an archive account.
    AccountUnmapped,
    /// The content type is wrong or the declared archive identity differs from the bytes.
    Invalid,
    /// The body exceeds the configured archive limit.
    TooLarge,
    /// The route does not accept the method.
    MethodNotAllowed,
    /// The archive could not be stored or recorded.
    Internal,
}

impl Kind {
    const ALL: [Self; 6] = [
        Self::Unauthenticated,
        Self::AccountUnmapped,
        Self::Invalid,
        Self::TooLarge,
        Self::MethodNotAllowed,
        Self::Internal,
    ];

    const fn status(self) -> StatusCode {
        match self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::AccountUnmapped => StatusCode::FORBIDDEN,
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::Unauthenticated => "claude.auth.unauthenticated",
            Self::AccountUnmapped => "claude.account.unmapped",
            Self::Invalid => "claude.request.invalid",
            Self::TooLarge => "claude.request.too_large",
            Self::MethodNotAllowed => "claude.request.method_not_allowed",
            Self::Internal => "claude.server.internal",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::Unauthenticated => "The request is not authenticated.",
            Self::AccountUnmapped => "This user is not set up for archive import.",
            Self::Invalid => "The request is invalid.",
            Self::TooLarge => "The archive exceeds the accepted size.",
            Self::MethodNotAllowed => "The method is not allowed for this route.",
            Self::Internal => "The archive could not be received.",
        }
    }

    /// The failure class logged at the boundary; never a path, claim or driver message.
    pub(crate) const fn class(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::AccountUnmapped => "unmapped_account",
            Self::Invalid => "invalid_request",
            Self::TooLarge => "payload_too_large",
            Self::MethodNotAllowed => "method_not_allowed",
            Self::Internal => "internal",
        }
    }
}

struct Fault {
    kind: Kind,
    body: Vec<u8>,
}

fn build() -> Option<Vec<Fault>> {
    Kind::ALL
        .into_iter()
        .map(|kind| {
            let envelope = ErrorEnvelope::new(
                ErrorCode::parse(kind.code()).ok()?,
                SafeMessage::parse(kind.message()).ok()?,
                false,
            );
            Some(Fault {
                kind,
                body: serde_json::to_vec(&envelope).ok()?,
            })
        })
        .collect()
}

fn table() -> Option<&'static [Fault]> {
    static TABLE: OnceLock<Option<Vec<Fault>>> = OnceLock::new();
    TABLE.get_or_init(build).as_deref()
}

/// Logs the failure class and the operation id once and renders the refusal as a JSON
/// `ErrorEnvelope`. The operation id is Platform's own identifier from the minted claims, when the
/// request carried a well-formed one; nothing about the archive is logged.
pub(crate) fn reject(kind: Kind, operation_id: Option<Uuid>) -> Response {
    tracing::warn!(
        class = kind.class(),
        operation_id = operation_id.map(|id| id.to_string()),
        "the Platform receipt was refused"
    );
    let mut response = match table().and_then(|faults| faults.iter().find(|f| f.kind == kind)) {
        Some(fault) => (kind.status(), fault.body.clone()).into_response(),
        // Unreachable while the static spellings satisfy their grammars, which a test guards.
        None => kind.status().into_response(),
    };
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if kind == Kind::MethodNotAllowed {
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("POST"));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::{Kind, table};

    #[test]
    fn every_static_spelling_satisfies_its_contract_grammar() {
        let faults = table().expect("the fault table builds from the contract grammars");
        assert_eq!(faults.len(), Kind::ALL.len());
    }
}
