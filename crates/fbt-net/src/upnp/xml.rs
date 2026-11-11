//! A bounded, iterative XML tag and text scanner. Not a conforming parser:
//! it only finds what UPnP device descriptions and SOAP replies need.

/// Services kept from a description.
const MAX_SERVICES: usize = 32;
/// Longest service type or control URL kept; a service with a longer one is
/// dropped.
const MAX_VALUE: usize = 256;
/// Open elements allowed; deeper documents are cut at that point.
const MAX_DEPTH: usize = 32;
/// Longest text returned by [`element_text`]; longer text gives `None`.
const MAX_TEXT: usize = 4096;

/// One `<service>` of a device description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Service {
    /// `serviceType`, e.g. `urn:schemas-upnp-org:service:WANIPConnection:1`.
    pub service_type: String,
    /// `controlURL`, as written (possibly relative).
    pub control_url: String,
}

enum Token<'a> {
    Start { name: &'a str, self_closing: bool },
    End,
    Text(&'a str),
}

struct Scanner<'a> {
    s: &'a str,
    pos: usize,
}

fn local(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

impl<'a> Scanner<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, pos: 0 }
    }

    /// Move to just past `end`, searching from `from`; `None` when it never
    /// comes.
    fn skip_past(&mut self, from: usize, end: &str) -> Option<()> {
        let rest = self.s.get(from..)?;
        self.pos = from + rest.find(end)? + end.len();
        Some(())
    }

    /// Index just past the `>` that closes a tag whose body starts at `from`,
    /// honouring quoted attribute values.
    fn tag_end(&self, from: usize) -> Option<usize> {
        let mut quote: Option<u8> = None;
        for (i, b) in self.s.get(from..)?.bytes().enumerate() {
            match (quote, b) {
                (Some(q), b) if b == q => quote = None,
                (Some(_), _) => {}
                (None, b'"' | b'\'') => quote = Some(b),
                (None, b'>') => return Some(from + i + 1),
                _ => {}
            }
        }
        None
    }
}

impl<'a> Iterator for Scanner<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Token<'a>> {
        loop {
            let rest = self.s.get(self.pos..)?;
            if rest.is_empty() {
                return None;
            }
            if !rest.starts_with('<') {
                let len = rest.find('<').unwrap_or(rest.len());
                self.pos += len;
                return Some(Token::Text(rest.get(..len)?));
            }
            let at = self.pos;
            if rest.starts_with("<!--") {
                self.skip_past(at + 4, "-->")?;
            } else if rest.starts_with("<![CDATA[") {
                self.skip_past(at + 9, "]]>")?;
            } else if rest.starts_with("<?") {
                self.skip_past(at + 2, "?>")?;
            } else if rest.starts_with("<!") {
                // DOCTYPE and friends: to the closing '>', past any [...] subset.
                let mut bracket = 0usize;
                let mut end = None;
                for (i, b) in rest.bytes().enumerate() {
                    match b {
                        b'[' => bracket += 1,
                        b']' => bracket = bracket.saturating_sub(1),
                        b'>' if bracket == 0 => {
                            end = Some(at + i + 1);
                            break;
                        }
                        _ => {}
                    }
                }
                self.pos = end?;
            } else if rest.starts_with("</") {
                self.pos = self.tag_end(at + 2)?;
                return Some(Token::End);
            } else {
                let end = self.tag_end(at + 1)?;
                let inner = self.s.get(at + 1..end.checked_sub(1)?)?;
                self.pos = end;
                let (inner, self_closing) = match inner.strip_suffix('/') {
                    Some(i) => (i, true),
                    None => (inner, false),
                };
                let name = inner.split(char::is_whitespace).next().unwrap_or_default();
                return Some(Token::Start { name, self_closing });
            }
        }
    }
}

fn decode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find('&') {
        out.push_str(rest.get(..i).unwrap_or_default());
        let tail = rest.get(i..).unwrap_or_default();
        let hit = [
            ("&lt;", '<'),
            ("&gt;", '>'),
            ("&amp;", '&'),
            ("&quot;", '"'),
            ("&apos;", '\''),
        ]
        .into_iter()
        .find(|(e, _)| tail.starts_with(e));
        match hit {
            Some((e, c)) => {
                out.push(c);
                rest = tail.get(e.len()..).unwrap_or_default();
            }
            None => {
                out.push('&');
                rest = tail.get(1..).unwrap_or_default();
            }
        }
    }
    out.push_str(rest);
    out
}

/// Every `<service>` at any depth with both a `serviceType` and a
/// `controlURL` child, at most 32 of them.
pub fn find_services(xml: &str) -> Vec<Service> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    // Depth of the open <service>, and the field being read with its depth.
    let mut service: Option<usize> = None;
    let mut field: Option<(usize, bool)> = None; // (depth, is_type)
    let (mut stype, mut curl) = (String::new(), String::new());
    for tok in Scanner::new(xml) {
        match tok {
            Token::Start { name, self_closing } => {
                if depth >= MAX_DEPTH {
                    break;
                }
                let name = local(name);
                match service {
                    None if name == "service" && !self_closing => {
                        service = Some(depth);
                        stype.clear();
                        curl.clear();
                    }
                    Some(sd) if depth == sd + 1 && field.is_none() && !self_closing => {
                        if name == "serviceType" {
                            field = Some((depth, true));
                        } else if name == "controlURL" {
                            field = Some((depth, false));
                        }
                    }
                    _ => {}
                }
                if !self_closing {
                    depth += 1;
                }
            }
            Token::Text(t) => {
                if let Some((_, is_type)) = field {
                    let buf = if is_type { &mut stype } else { &mut curl };
                    if buf.len() <= MAX_VALUE {
                        buf.push_str(&decode(t));
                    }
                }
            }
            Token::End => {
                depth = depth.saturating_sub(1);
                if field.is_some_and(|(d, _)| d == depth) {
                    field = None;
                    stype = stype.trim().to_owned();
                    curl = curl.trim().to_owned();
                } else if service == Some(depth) {
                    service = None;
                    let ok = |v: &str| !v.is_empty() && v.len() <= MAX_VALUE;
                    if ok(&stype) && ok(&curl) {
                        out.push(Service {
                            service_type: std::mem::take(&mut stype),
                            control_url: std::mem::take(&mut curl),
                        });
                        if out.len() >= MAX_SERVICES {
                            break;
                        }
                    }
                }
            }
        }
    }
    out
}

/// The text of the first element whose local name is `local_name`, with the
/// five predefined entities decoded. `None` when absent or over 4096 bytes.
pub fn element_text(xml: &str, local_name: &str) -> Option<String> {
    let mut depth = 0usize;
    let mut target: Option<usize> = None;
    let mut text = String::new();
    for tok in Scanner::new(xml) {
        match tok {
            Token::Start { name, self_closing } => {
                if depth >= MAX_DEPTH {
                    return None;
                }
                if target.is_none() && local(name) == local_name {
                    if self_closing {
                        return Some(String::new());
                    }
                    target = Some(depth);
                }
                if !self_closing {
                    depth += 1;
                }
            }
            Token::Text(t) => {
                // Direct text only: nested elements' text is not ours.
                if target.is_some_and(|d| d + 1 == depth) {
                    if text.len() + t.len() > MAX_TEXT {
                        return None;
                    }
                    text.push_str(t);
                }
            }
            Token::End => {
                depth = depth.saturating_sub(1);
                if target == Some(depth) {
                    return Some(decode(text.trim()));
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const ROOT_DESC: &str = r#"<?xml version="1.0"?>
<!DOCTYPE root [<!ENTITY x "y">]>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <specVersion><major>1</major><minor>0</minor></specVersion>
  <device>
    <deviceType>urn:schemas-upnp-org:device:InternetGatewayDevice:1</deviceType>
    <friendlyName>router &amp; co</friendlyName>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:Layer3Forwarding:1</serviceType>
        <controlURL>/ctl/L3F</controlURL>
      </service>
    </serviceList>
    <deviceList>
      <device>
        <deviceType>urn:schemas-upnp-org:device:WANDevice:1</deviceType>
        <deviceList>
          <device>
            <!-- <service> in a comment -->
            <serviceList>
              <service>
                <serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
                <serviceId>urn:upnp-org:serviceId:WANIPConn1</serviceId>
                <controlURL>/ctl/IPConn</controlURL>
                <eventSubURL>/evt/IPConn</eventSubURL>
                <SCPDURL>/WANIPCn.xml</SCPDURL>
              </service>
            </serviceList>
          </device>
        </deviceList>
      </device>
    </deviceList>
  </device>
</root>"#;

    #[test]
    fn finds_wanipconnection_in_miniupnpd_description() {
        let s = find_services(ROOT_DESC);
        assert_eq!(s.len(), 2);
        assert!(s.contains(&Service {
            service_type: "urn:schemas-upnp-org:service:WANIPConnection:1".into(),
            control_url: "/ctl/IPConn".into(),
        }));
    }

    #[test]
    fn element_text_ignores_prefix_and_decodes_entities() {
        let x = r#"<s:Envelope xmlns:s="x"><s:Body><u:Resp xmlns:u="y"><u:Ip>a &lt;b&gt; &amp; &quot;c&quot; &apos;d&apos; &nbsp; &amp</u:Ip></u:Resp></s:Body></s:Envelope>"#;
        assert_eq!(
            element_text(x, "Ip").as_deref(),
            Some("a <b> & \"c\" 'd' &nbsp; &amp")
        );
        assert_eq!(element_text(x, "Missing"), None);
        assert_eq!(
            element_text(ROOT_DESC, "friendlyName").as_deref(),
            Some("router & co")
        );
        assert_eq!(
            element_text("<a><![CDATA[<b>x</b>]]>t</a>", "a").as_deref(),
            Some("t")
        );
    }

    #[test]
    fn deep_nesting_is_cut() {
        let deep = "<a>".repeat(10_000);
        assert!(find_services(&deep).is_empty());
        assert_eq!(element_text(&deep, "zzz"), None);
        let svc = format!(
            "{}<service><serviceType>t</serviceType><controlURL>/c</controlURL></service>",
            "<a>".repeat(40)
        );
        assert!(find_services(&svc).is_empty());
    }

    #[test]
    fn caps_services_and_value_length() {
        let one = "<service><serviceType>t</serviceType><controlURL>/c</controlURL></service>";
        assert_eq!(find_services(&one.repeat(100)).len(), MAX_SERVICES);
        let long = format!(
            "<service><serviceType>t</serviceType><controlURL>{}</controlURL></service>",
            "x".repeat(MAX_VALUE + 1)
        );
        assert!(find_services(&long).is_empty());
    }

    proptest! {
        #[test]
        fn xml_scanner_never_panics(s in "\\PC{0,512}") {
            let _ = find_services(&s);
            let _ = element_text(&s, "a");
        }

        #[test]
        fn xml_scanner_never_panics_on_tag_soup(s in "[<>/!?\\[\\]\"'a-c&;:= -]{0,512}") {
            let _ = find_services(&s);
            let _ = element_text(&s, "a");
        }
    }
}
