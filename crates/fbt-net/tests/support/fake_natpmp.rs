//! A loopback gateway that speaks NAT-PMP only, PCP, or nothing.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

pub const EXTERNAL_IP: [u8; 4] = [203, 0, 113, 7];

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Answers PCP and NAT-PMP.
    Pcp,
    /// Answers NAT-PMP; says "unsupported version" to PCP.
    NatPmpOnly,
    /// Records requests and never answers.
    Silent,
}

pub struct FakeGateway {
    pub port: u16,
    pub requests: Arc<Mutex<Vec<Vec<u8>>>>,
    /// When each request in `requests` arrived (tokio's clock).
    pub times: Arc<Mutex<Vec<tokio::time::Instant>>>,
    /// While set, requests are recorded but never answered.
    pub mute: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl Drop for FakeGateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeGateway {
    /// `from_other_socket` sends every answer from a second socket; `code` is
    /// the result code of every answer.
    pub async fn start(mode: Mode, from_other_socket: bool, code: u16) -> FakeGateway {
        Self::spawn(mode, from_other_socket, code, false, None).await
    }

    /// Like `start` (answering, code 0), but every mapping is granted at
    /// most `lifetime` seconds, whatever was asked.
    pub async fn start_granting(mode: Mode, lifetime: u32) -> FakeGateway {
        Self::spawn(mode, false, 0, false, Some(lifetime)).await
    }

    /// Like `start`, but every NAT-PMP map request is first answered with a
    /// stale 8-byte "unsupported version" reply.
    pub async fn start_stale(mode: Mode) -> FakeGateway {
        Self::spawn(mode, false, 0, true, None).await
    }

    async fn spawn(
        mode: Mode,
        from_other_socket: bool,
        code: u16,
        stale: bool,
        grant: Option<u32>,
    ) -> FakeGateway {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = sock.local_addr().unwrap().port();
        let other = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = requests.clone();
        let times = Arc::new(Mutex::new(Vec::new()));
        let stamps = times.clone();
        let mute = Arc::new(AtomicBool::new(false));
        let muted = mute.clone();
        let task = tokio::spawn(async move {
            let mut buf = [0u8; 1100];
            while let Ok((n, src)) = sock.recv_from(&mut buf).await {
                let req = buf[..n].to_vec();
                log.lock().unwrap().push(req.clone());
                stamps.lock().unwrap().push(tokio::time::Instant::now());
                if muted.load(Ordering::SeqCst) {
                    continue;
                }
                if stale && req.first() == Some(&0) && matches!(req.get(1), Some(1 | 2)) {
                    let _ = sock.send_to(&[0, 129, 0, 1, 0, 0, 0, 5], src).await;
                }
                let Some(mut reply) = answer(mode, &req, code) else {
                    continue;
                };
                if let Some(g) = grant {
                    cap_lifetime(&mut reply, g);
                }
                let _ = if from_other_socket {
                    other.send_to(&reply, src).await
                } else {
                    sock.send_to(&reply, src).await
                };
            }
        });
        FakeGateway {
            port,
            requests,
            times,
            mute,
            task,
        }
    }

    pub fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    /// Every request with its arrival time.
    pub fn timed_requests(&self) -> Vec<(tokio::time::Instant, Vec<u8>)> {
        let times = self.times.lock().unwrap();
        let reqs = self.requests.lock().unwrap();
        times.iter().copied().zip(reqs.iter().cloned()).collect()
    }

    pub fn last_request(&self) -> Vec<u8> {
        self.requests.lock().unwrap().last().cloned().unwrap()
    }
}

fn answer(mode: Mode, req: &[u8], code: u16) -> Option<Vec<u8>> {
    match (mode, req.first().copied()?) {
        (Mode::Silent, _) => None,
        (Mode::NatPmpOnly, 2) => Some(vec![0, 129, 0, 1, 0, 0, 0, 5]),
        (Mode::Pcp, 2) if req.len() >= 60 => {
            let mut r = vec![0u8; 60];
            r[0] = 2;
            r[1] = 0x81;
            r[3] = code as u8;
            r[4..8].copy_from_slice(&req[4..8]);
            r[8..12].copy_from_slice(&9u32.to_be_bytes());
            r[24..44].copy_from_slice(&req[24..44]);
            r[42..44].copy_from_slice(&assigned(&req[42..44], &req[4..8]));
            r[54..56].copy_from_slice(&[0xff, 0xff]);
            r[56..60].copy_from_slice(&EXTERNAL_IP);
            Some(r)
        }
        (_, 0) => {
            let op = *req.get(1)?;
            let mut r = vec![0, 128 + op];
            r.extend_from_slice(&code.to_be_bytes());
            r.extend_from_slice(&9u32.to_be_bytes());
            if op == 0 {
                r.extend_from_slice(&EXTERNAL_IP);
            } else {
                r.extend_from_slice(req.get(4..6)?);
                r.extend_from_slice(&assigned(req.get(6..8)?, req.get(8..12)?));
                r.extend_from_slice(req.get(8..12)?);
            }
            Some(r)
        }
        _ => None,
    }
}

/// Lowers a non-zero lifetime in a map reply to `max`.
fn cap_lifetime(reply: &mut [u8], max: u32) {
    let at = match (reply.first(), reply.len()) {
        (Some(2), 60) => 4,
        (Some(0), 16) => 12,
        _ => return,
    };
    let life = u32::from_be_bytes(reply[at..at + 4].try_into().unwrap());
    if life != 0 {
        reply[at..at + 4].copy_from_slice(&life.min(max).to_be_bytes());
    }
}

/// The external port granted: the suggestion plus one, except for deletes
/// (lifetime 0), which echo it.
fn assigned(suggested: &[u8], lifetime: &[u8]) -> [u8; 2] {
    let s = u16::from_be_bytes([suggested[0], suggested[1]]);
    if lifetime == [0, 0, 0, 0] {
        s.to_be_bytes()
    } else {
        s.wrapping_add(1).to_be_bytes()
    }
}
