//! SOAP request builders and the reply parser for the IGD actions we use.

use std::net::Ipv4Addr;

use super::{HttpResponse, UpnpError, element_text};
use crate::MapProtocol;
use crate::upnp::HttpError;

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

fn proto(p: MapProtocol) -> &'static str {
    match p {
        MapProtocol::Tcp => "TCP",
        MapProtocol::Udp => "UDP",
    }
}

/// The `SOAPAction` header value and the envelope for `action`; `args` are
/// `(element, value)` pairs, values escaped here.
fn envelope(service_type: &str, action: &str, args: &[(&str, String)]) -> (String, Vec<u8>) {
    let st = escape(service_type);
    let mut body = format!(
        "<?xml version=\"1.0\"?>\r\n<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body><u:{action} xmlns:u=\"{st}\">"
    );
    for (name, value) in args {
        body.push_str(&format!("<{name}>{}</{name}>", escape(value)));
    }
    body.push_str(&format!("</u:{action}></s:Body></s:Envelope>"));
    (format!("\"{service_type}#{action}\""), body.into_bytes())
}

/// `AddPortMapping`: `(SOAPAction header value, body)`.
pub fn add_port_mapping(
    service_type: &str,
    ext_port: u16,
    p: MapProtocol,
    int_port: u16,
    client: Ipv4Addr,
    lease: u32,
    description: &str,
) -> (String, Vec<u8>) {
    envelope(
        service_type,
        "AddPortMapping",
        &[
            ("NewRemoteHost", String::new()),
            ("NewExternalPort", ext_port.to_string()),
            ("NewProtocol", proto(p).to_owned()),
            ("NewInternalPort", int_port.to_string()),
            ("NewInternalClient", client.to_string()),
            ("NewEnabled", "1".to_owned()),
            ("NewPortMappingDescription", description.to_owned()),
            ("NewLeaseDuration", lease.to_string()),
        ],
    )
}

/// `DeletePortMapping`.
pub fn delete_port_mapping(service_type: &str, ext_port: u16, p: MapProtocol) -> (String, Vec<u8>) {
    envelope(
        service_type,
        "DeletePortMapping",
        &[
            ("NewRemoteHost", String::new()),
            ("NewExternalPort", ext_port.to_string()),
            ("NewProtocol", proto(p).to_owned()),
        ],
    )
}

/// `GetExternalIPAddress`.
pub fn get_external_ip(service_type: &str) -> (String, Vec<u8>) {
    envelope(service_type, "GetExternalIPAddress", &[])
}

/// Interpret a SOAP reply: a 200 is success (with `NewExternalIPAddress` if
/// present); a 500 with an `errorCode` is `UpnpError::Soap`.
pub fn soap_result(resp: &HttpResponse) -> Result<Option<String>, UpnpError> {
    let text = String::from_utf8_lossy(&resp.body);
    match resp.status {
        200 => Ok(element_text(&text, "NewExternalIPAddress")),
        500 => match element_text(&text, "errorCode").and_then(|c| c.trim().parse::<u32>().ok()) {
            Some(code) => Err(UpnpError::Soap(code)),
            None => Err(HttpError::Malformed("soap fault without error code").into()),
        },
        _ => Err(HttpError::Malformed("unexpected soap status").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ST: &str = "urn:schemas-upnp-org:service:WANIPConnection:1";

    #[test]
    fn add_port_mapping_body_matches_spec() {
        let (action, body) = add_port_mapping(
            ST,
            6882,
            MapProtocol::Tcp,
            6881,
            Ipv4Addr::new(192, 168, 1, 5),
            3600,
            "fbt",
        );
        assert_eq!(
            action,
            "\"urn:schemas-upnp-org:service:WANIPConnection:1#AddPortMapping\""
        );
        let body = String::from_utf8(body).unwrap();
        for want in [
            "<u:AddPortMapping xmlns:u=\"urn:schemas-upnp-org:service:WANIPConnection:1\">",
            "<NewExternalPort>6882</NewExternalPort>",
            "<NewProtocol>TCP</NewProtocol>",
            "<NewInternalPort>6881</NewInternalPort>",
            "<NewInternalClient>192.168.1.5</NewInternalClient>",
            "<NewLeaseDuration>3600</NewLeaseDuration>",
            "<NewPortMappingDescription>fbt</NewPortMappingDescription>",
        ] {
            assert!(body.contains(want), "{want} in {body}");
        }
    }

    #[test]
    fn values_are_xml_escaped() {
        let (_, body) = add_port_mapping(
            "a\"<b>&'",
            1,
            MapProtocol::Udp,
            1,
            Ipv4Addr::LOCALHOST,
            0,
            "<x>&\"'",
        );
        let body = String::from_utf8(body).unwrap();
        assert!(body.contains("xmlns:u=\"a&quot;&lt;b&gt;&amp;&apos;\""));
        assert!(body.contains("<NewPortMappingDescription>&lt;x&gt;&amp;&quot;&apos;<"));
        assert!(!body.contains("<x>"));
    }

    fn resp(status: u16, body: &str) -> HttpResponse {
        HttpResponse {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn soap_fault_code_extracted() {
        let fault = "<s:Envelope><s:Body><s:Fault><detail><UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>718</errorCode><errorDescription>ConflictInMappingEntry</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>";
        assert!(matches!(
            soap_result(&resp(500, fault)),
            Err(UpnpError::Soap(718))
        ));
        assert!(matches!(
            soap_result(&resp(500, "<x/>")),
            Err(UpnpError::Http(_))
        ));
        assert!(matches!(
            soap_result(&resp(404, "")),
            Err(UpnpError::Http(_))
        ));
        let ok = "<u:R><NewExternalIPAddress>1.2.3.4</NewExternalIPAddress></u:R>";
        assert_eq!(
            soap_result(&resp(200, ok)).unwrap().as_deref(),
            Some("1.2.3.4")
        );
        assert_eq!(soap_result(&resp(200, "<u:R/>")).unwrap(), None);
    }
}
