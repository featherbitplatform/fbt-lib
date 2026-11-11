#![no_main]
use fbt_net::upnp::{element_text, find_services, parse_response};
use libfuzzer_sys::fuzz_target;

// Device descriptions and SOAP bodies are XML; HTTP framing wraps them.
fuzz_target!(|data: &[u8]| {
    let _ = parse_response(data);
    if let Ok(xml) = std::str::from_utf8(data) {
        let _ = find_services(xml);
        let _ = element_text(xml, "errorCode");
        let _ = element_text(xml, "NewExternalIPAddress");
    }
});
