//! UPnP IGD plumbing: a bounded HTTP/1.1 client and a bounded XML scanner.
//!
//! Device replies are untrusted: every list, line and buffer here has a named
//! cap, every exchange a timeout.

mod http;
mod xml;

#[doc(hidden)]
pub use http::{
    HTTP_TIMEOUT, HttpError, HttpRequest, HttpResponse, MAX_HTTP_RESPONSE, parse_response, request,
    request_with_timeout,
};
#[doc(hidden)]
pub use xml::{Service, element_text, find_services};
