//! A loopback IGD: a UDP responder standing in for the SSDP multicast group,
//! plus an HTTP server for the description and SOAP control.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::task::JoinHandle;

pub const EXTERNAL_IP: &str = "203.0.113.9";
pub const SERVICE: &str = "urn:schemas-upnp-org:service:WANIPConnection:1";

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum Description {
    #[default]
    Normal,
    /// A body larger than the client's cap.
    Oversized,
    /// Headers, then silence.
    Slow,
}

#[derive(Clone, Default)]
pub struct Script {
    /// AddPortMapping on these external ports fails with 718.
    pub conflict_ports: Vec<u16>,
    /// AddPortMapping with a non-zero lease fails with 725.
    pub permanent_only: bool,
    /// SSDP is answered from `[::1]` while LOCATION names the fake's HTTP
    /// listener on 127.0.0.1: a client that connects anyway is caught by the
    /// connection count. Callers check that `[::1]` is bindable first.
    pub foreign_location: bool,
    pub description: Description,
}

#[derive(Clone, Debug)]
pub struct Recorded {
    pub action: String,
    pub body: String,
}

impl Recorded {
    /// The text of `<name>` in the SOAP body.
    pub fn arg(&self, name: &str) -> String {
        let open = format!("<{name}>");
        let start = self.body.find(&open).map(|i| i + open.len()).unwrap_or(0);
        let rest = self.body.get(start..).unwrap_or_default();
        rest.split('<').next().unwrap_or_default().to_owned()
    }
}

pub struct FakeIgd {
    /// Where `discover` should send its searches.
    pub ssdp: SocketAddr,
    pub http: SocketAddr,
    pub actions: Arc<Mutex<Vec<Recorded>>>,
    pub http_connections: Arc<AtomicUsize>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for FakeIgd {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl FakeIgd {
    pub async fn start(script: Script) -> FakeIgd {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = listener.local_addr().unwrap();
        let udp = UdpSocket::bind(if script.foreign_location {
            "[::1]:0"
        } else {
            "127.0.0.1:0"
        })
        .await
        .unwrap();
        let ssdp = udp.local_addr().unwrap();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let http_connections = Arc::new(AtomicUsize::new(0));

        let location_host = http.ip().to_string();
        let reply = format!(
            "HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age=120\r\nLOCATION: http://{location_host}:{}/rootDesc.xml\r\nST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\r\n",
            http.port()
        );
        let ssdp_task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            while let Ok((n, from)) = udp.recv_from(&mut buf).await {
                if buf.get(..n).is_some_and(|d| d.starts_with(b"M-SEARCH")) {
                    let _ = udp.send_to(reply.as_bytes(), from).await;
                }
            }
        });

        let (acts, conns) = (actions.clone(), http_connections.clone());
        let http_task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                conns.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve(stream, script.clone(), acts.clone()));
            }
        });
        FakeIgd {
            ssdp,
            http,
            actions,
            http_connections,
            tasks: vec![ssdp_task, http_task],
        }
    }

    pub fn recorded(&self, action: &str) -> Vec<Recorded> {
        self.actions
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.action == action)
            .cloned()
            .collect()
    }
}

async fn read_request(stream: &mut TcpStream) -> Option<(String, String)> {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 2048];
    loop {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        raw.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&raw).into_owned();
        if let Some(i) = text.find("\r\n\r\n") {
            let (head, body) = (&text[..i], &text[i + 4..]);
            let len = head
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                })
                .unwrap_or(0);
            if body.len() >= len {
                return Some((head.to_owned(), body.to_owned()));
            }
        }
    }
}

async fn respond(stream: &mut TcpStream, status: &str, body: &str) {
    let msg = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(msg.as_bytes()).await;
}

fn fault(code: u32) -> String {
    format!(
        "<s:Envelope><s:Body><s:Fault><detail><UPnPError><errorCode>{code}</errorCode></UPnPError></detail></s:Fault></s:Body></s:Envelope>"
    )
}

async fn serve(mut stream: TcpStream, script: Script, actions: Arc<Mutex<Vec<Recorded>>>) {
    let Some((head, body)) = read_request(&mut stream).await else {
        return;
    };
    if head.starts_with("GET /rootDesc.xml") {
        match script.description {
            Description::Oversized => {
                let msg = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", 1_000_000);
                let _ = stream.write_all(msg.as_bytes()).await;
                let _ = stream.write_all(&vec![b'a'; 70_000]).await;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Description::Slow => {
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n")
                    .await;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Description::Normal => {
                let xml = format!(
                    "<?xml version=\"1.0\"?><root xmlns=\"urn:schemas-upnp-org:device-1-0\"><device><deviceList><device><deviceList><device><serviceList><service><serviceType>urn:schemas-upnp-org:service:WANCommonInterfaceConfig:1</serviceType><controlURL>/ctl/CmnIfCfg</controlURL></service><service><serviceType>{SERVICE}</serviceType><controlURL>/ctl/IPConn</controlURL></service></serviceList></device></deviceList></device></deviceList></device></root>"
                );
                respond(&mut stream, "200 OK", &xml).await;
            }
        }
        return;
    }
    if head.starts_with("POST /ctl/IPConn") {
        let action = head
            .lines()
            .find_map(|l| {
                let (n, v) = l.split_once(':')?;
                n.eq_ignore_ascii_case("soapaction").then(|| v.trim())
            })
            .and_then(|v| v.trim_matches('"').split('#').nth(1))
            .unwrap_or_default()
            .to_owned();
        let rec = Recorded {
            action: action.clone(),
            body,
        };
        let ext: u16 = rec.arg("NewExternalPort").parse().unwrap_or(0);
        let lease: u32 = rec.arg("NewLeaseDuration").parse().unwrap_or(0);
        actions.lock().unwrap().push(rec);
        match action.as_str() {
            "AddPortMapping" if script.conflict_ports.contains(&ext) => {
                respond(&mut stream, "500 Internal Server Error", &fault(718)).await;
            }
            "AddPortMapping" if script.permanent_only && lease != 0 => {
                respond(&mut stream, "500 Internal Server Error", &fault(725)).await;
            }
            "GetExternalIPAddress" => {
                let body = format!(
                    "<s:Envelope><s:Body><u:GetExternalIPAddressResponse><NewExternalIPAddress>{EXTERNAL_IP}</NewExternalIPAddress></u:GetExternalIPAddressResponse></s:Body></s:Envelope>"
                );
                respond(&mut stream, "200 OK", &body).await;
            }
            _ => respond(&mut stream, "200 OK", "<s:Envelope/>").await,
        }
        return;
    }
    respond(&mut stream, "404 Not Found", "").await;
}
