//! Runs the built `fbt-example` binary.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fbt::Metainfo;
use fbt_bencode::{DictBuilder, OwnedValue, encode};
use sha1::{Digest, Sha1};

const BIN: &str = env!("CARGO_BIN_EXE_fbt-example");

#[test]
fn info_prints_info_hash() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../fbt-metainfo/tests/fixtures/single.torrent"
    );
    let m = Metainfo::from_bytes(&std::fs::read(path).expect("fixture")).expect("parse");
    let out = Command::new(BIN)
        .args(["info", path])
        .output()
        .expect("run");
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains(&m.info_hashes.to_string()), "{stdout}");
    assert!(stdout.contains(&m.info.name), "{stdout}");
}

fn make_torrent(files: &[(&str, usize)], piece_len: usize) -> (Vec<u8>, Vec<u8>) {
    let payload: Vec<u8> = (0..files.iter().map(|f| f.1).sum::<usize>())
        .map(|i| (i.wrapping_mul(31) ^ (i >> 7)) as u8)
        .collect();
    let pieces: Vec<u8> = payload
        .chunks(piece_len)
        .flat_map(|c| Sha1::digest(c).to_vec())
        .collect();
    let list = files
        .iter()
        .map(|(name, len)| {
            let mut f = DictBuilder::new();
            f.insert(b"length", *len as i64);
            f.insert(
                b"path",
                OwnedValue::List(name.split('/').map(OwnedValue::from).collect::<Vec<_>>()),
            );
            f.build()
        })
        .collect();
    let mut info = DictBuilder::new();
    info.insert(b"files", OwnedValue::List(list));
    info.insert(b"name", "payload");
    info.insert(b"piece length", piece_len as i64);
    info.insert(b"pieces", pieces);
    let mut root = DictBuilder::new();
    root.insert(b"info", info.build());
    (encode(&root.build()), payload)
}

struct Kill(Child);
impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_exit(child: &mut Child, limit: Duration) -> std::process::ExitStatus {
    let start = Instant::now();
    loop {
        if let Some(s) = child.try_wait().expect("try_wait") {
            return s;
        }
        assert!(start.elapsed() < limit, "download did not exit in time");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn read_tree(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).expect("read_dir") {
            let p = e.expect("entry").path();
            if p.is_dir() {
                stack.push(p);
            } else {
                let rel = p.strip_prefix(dir).expect("prefix").display().to_string();
                out.push((rel, std::fs::read(&p).expect("read")));
            }
        }
    }
    out.sort();
    out
}

/// Starts `seed` (no DHT, port 0) and returns it with its listening port.
fn spawn_seed(torrent: &Path, data: &Path, extra: &[&str]) -> (Kill, String) {
    let mut seed = Kill(
        Command::new(BIN)
            .args(["seed"])
            .arg(torrent)
            .arg("--data")
            .arg(data)
            .args(["--port", "0", "--no-dht"])
            .args(extra)
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn seed"),
    );
    let stdout = seed.0.stdout.take().expect("stdout");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut lines = BufReader::new(stdout);
        let mut first = String::new();
        let _ = lines.read_line(&mut first);
        let _ = tx.send(first);
        // Keep draining so the seed never blocks on a full pipe.
        let mut rest = String::new();
        while lines.read_line(&mut rest).map(|n| n > 0).unwrap_or(false) {
            rest.clear();
        }
    });
    let first = rx
        .recv_timeout(Duration::from_secs(60))
        .expect("seed printed its listening line");
    let addr = first
        .trim()
        .strip_prefix("listening on ")
        .unwrap_or_else(|| panic!("unexpected first line {first:?}"))
        .to_string();
    let port = addr.rsplit(':').next().expect("port").to_string();
    (seed, port)
}

/// Writes the test torrent and the seed's data; returns (torrent path, seed dir, v1 hash).
fn seed_fixture(tmp: &Path) -> (std::path::PathBuf, std::path::PathBuf, String) {
    let files = [("a.bin", 100_000), ("sub/b.bin", 70_001), ("c.bin", 5)];
    let (torrent, payload) = make_torrent(&files, 16384);
    let torrent_path = tmp.join("t.torrent");
    std::fs::write(&torrent_path, &torrent).expect("write torrent");
    let seed_dir = tmp.join("seed");
    let mut off = 0;
    for (name, len) in files {
        let p = seed_dir.join("payload").join(name);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(&p, &payload[off..off + len]).expect("write");
        off += len;
    }
    let hash = Metainfo::from_bytes(&torrent)
        .expect("parse")
        .info_hashes
        .v1
        .expect("v1")
        .to_string();
    (torrent_path, seed_dir, hash)
}

/// Runs `download` to completion (60 s limit); returns its stdout.
fn run_download(args: &[&str]) -> String {
    let mut dl = Kill(
        Command::new(BIN)
            .arg("download")
            .args(args)
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn download"),
    );
    let mut stdout = dl.0.stdout.take().expect("stdout");
    let reader = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = std::io::Read::read_to_string(&mut stdout, &mut text);
        text
    });
    let status = wait_exit(&mut dl.0, Duration::from_secs(60));
    let text = reader.join().expect("reader");
    assert!(status.success(), "{status:?}\n{text}");
    text
}

#[test]
fn info_prints_magnet_link() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../fbt-metainfo/tests/fixtures/single.torrent"
    );
    let m = Metainfo::from_bytes(&std::fs::read(path).expect("fixture")).expect("parse");
    let out = Command::new(BIN)
        .args(["info", path])
        .output()
        .expect("run");
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout
        .lines()
        .find_map(|l| l.strip_prefix("magnet: "))
        .unwrap_or_else(|| panic!("no magnet line in {stdout}"));
    let parsed = fbt::Magnet::parse(line).expect("magnet parses");
    assert_eq!(parsed.info_hashes, m.info_hashes);
}

#[test]
fn download_magnet_from_seed_process() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (torrent_path, seed_dir, hash) = seed_fixture(tmp.path());
    let out_dir = tmp.path().join("out");
    let (_seed, port) = spawn_seed(&torrent_path, &seed_dir, &[]);
    let magnet = format!("magnet:?xt=urn:btih:{hash}&x.pe=127.0.0.1:{port}");
    let stdout = run_download(&[
        &magnet,
        "--no-dht",
        "--port",
        "0",
        "--out",
        out_dir.to_str().expect("utf8"),
    ]);
    assert!(stdout.contains("metadata received:"), "{stdout}");
    assert_eq!(
        read_tree(&out_dir.join("payload")),
        read_tree(&seed_dir.join("payload"))
    );
}

#[test]
fn encryption_forced_download_from_seed_process() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (torrent_path, seed_dir, _) = seed_fixture(tmp.path());
    let out_dir = tmp.path().join("out");
    let (_seed, port) = spawn_seed(&torrent_path, &seed_dir, &["--encryption", "forced"]);
    let peer = format!("127.0.0.1:{port}");
    run_download(&[
        torrent_path.to_str().expect("utf8"),
        "--no-dht",
        "--port",
        "0",
        "--encryption",
        "forced",
        "--peer",
        &peer,
        "--out",
        out_dir.to_str().expect("utf8"),
    ]);
    assert_eq!(
        read_tree(&out_dir.join("payload")),
        read_tree(&seed_dir.join("payload"))
    );
}

#[test]
fn dht_node_starts_and_stops() {
    let out = Command::new(BIN)
        .args(["dht-node", "--port", "0", "--run-for", "1"])
        .output()
        .expect("run");
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("dht node listening on"), "{stdout}");
}

#[test]
fn bad_magnet_is_a_clean_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = Command::new(BIN)
        .args(["download", "magnet:?dn=nothing", "--no-dht", "--out"])
        .arg(tmp.path())
        .output()
        .expect("run");
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.starts_with("error:"), "{stderr}");
}

#[test]
fn download_from_seed_process() {
    let files = [("a.bin", 100_000), ("sub/b.bin", 70_001), ("c.bin", 5)];
    let (torrent, payload) = make_torrent(&files, 16384);
    let tmp = tempfile::tempdir().expect("tempdir");
    let torrent_path = tmp.path().join("t.torrent");
    std::fs::write(&torrent_path, &torrent).expect("write torrent");
    let seed_dir = tmp.path().join("seed");
    let out_dir = tmp.path().join("out");
    let mut off = 0;
    for (name, len) in files {
        let p = seed_dir.join("payload").join(name);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(&p, &payload[off..off + len]).expect("write");
        off += len;
    }

    let (_seed, port) = spawn_seed(&torrent_path, &seed_dir, &[]);

    let mut dl = Kill(
        Command::new(BIN)
            .arg("download")
            .arg(&torrent_path)
            .arg("--out")
            .arg(&out_dir)
            .args(["--port", "0", "--no-dht", "--peer"])
            .arg(format!("127.0.0.1:{port}"))
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn download"),
    );
    let status = wait_exit(&mut dl.0, Duration::from_secs(60));
    assert!(status.success(), "{status:?}");

    let got = read_tree(&out_dir.join("payload"));
    let want = read_tree(&seed_dir.join("payload"));
    assert_eq!(got, want);
}

#[test]
fn download_over_utp_from_seed_process() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (torrent_path, seed_dir, _) = seed_fixture(tmp.path());
    let out_dir = tmp.path().join("out");
    let (_seed, port) = spawn_seed(&torrent_path, &seed_dir, &["--transport", "utp"]);
    let peer = format!("127.0.0.1:{port}");
    let stdout = run_download(&[
        torrent_path.to_str().expect("utf8"),
        "--no-dht",
        "--port",
        "0",
        "--transport",
        "utp",
        "--peer",
        &peer,
        "--out",
        out_dir.to_str().expect("utf8"),
    ]);
    assert!(stdout.contains("via utp"), "{stdout}");
    assert_eq!(
        read_tree(&out_dir.join("payload")),
        read_tree(&seed_dir.join("payload"))
    );
}

#[test]
fn download_limit_slows_transfer() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let payload: Vec<u8> = (0..4 * 1024 * 1024usize)
        .map(|i| (i.wrapping_mul(31) ^ (i >> 7)) as u8)
        .collect();
    let (torrent, _) = make_torrent(&[("big.bin", payload.len())], 65536);
    let torrent_path = tmp.path().join("t.torrent");
    std::fs::write(&torrent_path, &torrent).expect("write torrent");
    let seed_dir = tmp.path().join("seed");
    std::fs::create_dir_all(seed_dir.join("payload")).expect("mkdir");
    std::fs::write(seed_dir.join("payload/big.bin"), &payload).expect("write");
    let out_dir = tmp.path().join("out");
    let (_seed, port) = spawn_seed(&torrent_path, &seed_dir, &[]);
    let peer = format!("127.0.0.1:{port}");
    let start = Instant::now();
    run_download(&[
        torrent_path.to_str().expect("utf8"),
        "--no-dht",
        "--port",
        "0",
        "--download-limit",
        "1M",
        "--peer",
        &peer,
        "--out",
        out_dir.to_str().expect("utf8"),
    ]);
    assert!(
        start.elapsed() >= Duration::from_secs(3),
        "finished in {:?}",
        start.elapsed()
    );
    assert_eq!(
        std::fs::read(out_dir.join("payload/big.bin")).expect("read"),
        payload
    );
}

#[test]
fn bad_limit_is_a_clean_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = Command::new(BIN)
        .args([
            "download",
            "magnet:?dn=x",
            "--download-limit",
            "12Q",
            "--out",
        ])
        .arg(tmp.path())
        .output()
        .expect("run");
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("12Q"), "{stderr}");
}

#[test]
fn listen_flag_accepts_two_addresses() {
    if std::net::TcpListener::bind("[::1]:0").is_err() {
        return;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let (torrent_path, seed_dir, _) = seed_fixture(tmp.path());
    let out_dir = tmp.path().join("out");
    let (_seed, port) = spawn_seed(
        &torrent_path,
        &seed_dir,
        &["--listen", "127.0.0.1:0", "--listen", "[::1]:0"],
    );
    let peer = format!("127.0.0.1:{port}");
    run_download(&[
        torrent_path.to_str().expect("utf8"),
        "--no-dht",
        "--port",
        "0",
        "--peer",
        &peer,
        "--out",
        out_dir.to_str().expect("utf8"),
    ]);
    assert_eq!(
        read_tree(&out_dir.join("payload")),
        read_tree(&seed_dir.join("payload"))
    );
}

/// Runs `stream` (60 s limit) with stdout and stderr captured.
fn run_stream(args: &[&str]) -> std::process::Output {
    let child = Command::new(BIN)
        .arg("stream")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn stream");
    let mut child = Kill(child);
    let mut stdout = child.0.stdout.take().expect("stdout");
    let mut stderr = child.0.stderr.take().expect("stderr");
    let out_thread = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = std::io::Read::read_to_end(&mut stdout, &mut v);
        v
    });
    let err_thread = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = std::io::Read::read_to_end(&mut stderr, &mut v);
        v
    });
    let status = wait_exit(&mut child.0, Duration::from_secs(60));
    std::process::Output {
        status,
        stdout: out_thread.join().expect("stdout reader"),
        stderr: err_thread.join().expect("stderr reader"),
    }
}

#[test]
fn stream_to_a_path_matches_the_file() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (torrent_path, seed_dir, _) = seed_fixture(tmp.path());
    let (_seed, port) = spawn_seed(&torrent_path, &seed_dir, &[]);
    let peer = format!("127.0.0.1:{port}");
    let out_dir = tmp.path().join("out");
    let to = tmp.path().join("out.bin");
    let out = run_stream(&[
        torrent_path.to_str().expect("utf8"),
        "--file",
        "1",
        "--out",
        out_dir.to_str().expect("utf8"),
        "--port",
        "0",
        "--no-dht",
        "--to",
        to.to_str().expect("utf8"),
        "--peer",
        &peer,
    ]);
    assert!(out.status.success(), "{out:?}");
    assert_eq!(
        std::fs::read(&to).expect("out.bin"),
        std::fs::read(seed_dir.join("payload/sub/b.bin")).expect("file 1")
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("stream: 70001 bytes from 0 in "),
        "{stderr}"
    );
}

#[test]
fn stream_with_seek_and_length_to_stdout() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (torrent_path, seed_dir, _) = seed_fixture(tmp.path());
    let (_seed, port) = spawn_seed(&torrent_path, &seed_dir, &[]);
    let peer = format!("127.0.0.1:{port}");
    let out_dir = tmp.path().join("out");
    let out = run_stream(&[
        torrent_path.to_str().expect("utf8"),
        "--file",
        "0",
        "--out",
        out_dir.to_str().expect("utf8"),
        "--port",
        "0",
        "--no-dht",
        "--seek",
        "30000",
        "--length",
        "70000",
        "--peer",
        &peer,
    ]);
    assert!(out.status.success(), "{out:?}");
    let file = std::fs::read(seed_dir.join("payload/a.bin")).expect("file 0");
    assert_eq!(out.stdout, file[30000..100_000]);
}

#[test]
fn stream_bad_file_index_is_a_clean_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (torrent_path, _seed_dir, _) = seed_fixture(tmp.path());
    let out_dir = tmp.path().join("out");
    let out = run_stream(&[
        torrent_path.to_str().expect("utf8"),
        "--file",
        "9",
        "--out",
        out_dir.to_str().expect("utf8"),
        "--port",
        "0",
        "--no-dht",
    ]);
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("file 9"), "{stderr}");
}

#[path = "../../fbt-session/tests/support/web_seed.rs"]
mod web_seed;

/// Runs `download` (60 s limit) with stdout and stderr captured.
fn run_download_output(args: &[&str]) -> std::process::Output {
    let child = Command::new(BIN)
        .arg("download")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn download");
    let mut child = Kill(child);
    let mut stdout = child.0.stdout.take().expect("stdout");
    let mut stderr = child.0.stderr.take().expect("stderr");
    let out_thread = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = std::io::Read::read_to_end(&mut stdout, &mut v);
        v
    });
    let err_thread = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = std::io::Read::read_to_end(&mut stderr, &mut v);
        v
    });
    let status = wait_exit(&mut child.0, Duration::from_secs(60));
    std::process::Output {
        status,
        stdout: out_thread.join().expect("stdout reader"),
        stderr: err_thread.join().expect("stderr reader"),
    }
}

/// `--proxy` takes `socks5://` and `http://` URLs with optional
/// credentials (the bad magnet then fails after parsing, exit 1); a URL
/// without a port, with another scheme or with no host is a usage error
/// (exit 2) naming the flag.
#[test]
fn proxy_url_parsing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let run = |proxy: &str| {
        Command::new(BIN)
            .args(["download", "magnet:?dn=nothing", "--port", "0", "--no-dht"])
            .args(["--proxy", proxy, "--out"])
            .arg(tmp.path())
            .output()
            .expect("run")
    };
    for good in [
        "socks5://127.0.0.1:1080",
        "socks5://user:pass@proxy.example:1080",
        "http://127.0.0.1:3128",
        "http://user:p%40ss@[::1]:8080",
        "http://user@proxy.example:8080",
    ] {
        let out = run(good);
        assert_eq!(out.status.code(), Some(1), "{good}: {out:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.starts_with("error:"), "{good}: {stderr}");
    }
    for bad in [
        "socks5://127.0.0.1",
        "http://user:hunter2@proxy.example",
        "socks4://user:hunter2@127.0.0.1:1080",
        "socks4://127.0.0.1:1080",
        "127.0.0.1:1080",
        "socks5://:1080",
        "socks5://127.0.0.1:99999",
        "http://127.0.0.1:3128/path",
    ] {
        let out = run(bad);
        assert_eq!(out.status.code(), Some(2), "{bad}: {out:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains("--proxy"), "{bad}: {stderr}");
        // A rejected URL's password never reaches the terminal or CI logs.
        assert!(!stderr.contains("hunter2"), "{bad}: {stderr}");
    }
}

/// `--stop-at 50 --resume` pauses at half, saves the resume file and exits
/// 0; a second run from that file trusts the saved pieces (no check) and
/// finishes.
#[test]
fn resume_file_is_written_and_trusted() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // 2 MiB at 256 KiB/s: about 4 s to the halfway mark and 12.5 % per 1 s
    // progress poll, so the stop lands well short of 100 %.
    let payload: Vec<u8> = (0..2 * 1024 * 1024usize)
        .map(|i| (i.wrapping_mul(31) ^ (i >> 7)) as u8)
        .collect();
    let (torrent, _) = make_torrent(&[("big.bin", payload.len())], 65536);
    let torrent_path = tmp.path().join("t.torrent");
    std::fs::write(&torrent_path, &torrent).expect("write torrent");
    let seed_dir = tmp.path().join("seed");
    std::fs::create_dir_all(seed_dir.join("payload")).expect("mkdir");
    std::fs::write(seed_dir.join("payload/big.bin"), &payload).expect("write");
    let out_dir = tmp.path().join("out");
    let resume = tmp.path().join("r.dat");
    let (_seed, port) = spawn_seed(&torrent_path, &seed_dir, &[]);
    let peer = format!("127.0.0.1:{port}");
    let args = [
        torrent_path.to_str().expect("utf8"),
        "--port",
        "0",
        "--no-dht",
        "--peer",
        &peer,
        "--out",
        out_dir.to_str().expect("utf8"),
        "--resume",
        resume.to_str().expect("utf8"),
    ];

    let mut first = args.to_vec();
    first.extend(["--stop-at", "50", "--download-limit", "256K"]);
    let out = run_download_output(&first);
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stopped_at: f64 = stdout
        .lines()
        .find_map(|l| l.strip_prefix("stop at 50%: "))
        .and_then(|rest| rest.strip_suffix("% done"))
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or_else(|| panic!("no stop line in {stdout}"));
    assert!(
        (50.0..100.0).contains(&stopped_at),
        "stopped at {stopped_at}%: {stdout}"
    );
    assert!(resume.is_file(), "no resume file written");
    let saved = std::fs::read(&resume).expect("resume file");
    assert!(fbt::AddTorrent::from_resume(&saved).is_ok());
    // The temp file is renamed into place, not left behind.
    let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("r.dat") && n != "r.dat")
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");

    let out = run_download_output(&args);
    assert!(out.status.success(), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("resume: trusted ("), "{stderr}");
    assert!(!stderr.contains("resume: checking"), "{stderr}");
    assert_eq!(
        std::fs::read(out_dir.join("payload/big.bin")).expect("read"),
        payload
    );
}

/// `--web-seed` alone (no peer, no tracker, no DHT) downloads the torrent
/// from a loopback HTTP server.
#[test]
fn web_seed_flag_downloads() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (torrent_path, seed_dir, _) = seed_fixture(tmp.path());
    let out_dir = tmp.path().join("out");
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let server = rt.block_on(web_seed::WebSeedServer::start(
        seed_dir.clone(),
        web_seed::Behaviour::Normal,
    ));
    let url = server.url();
    let out = run_download_output(&[
        torrent_path.to_str().expect("utf8"),
        "--port",
        "0",
        "--no-dht",
        "--web-seed",
        &url,
        "--out",
        out_dir.to_str().expect("utf8"),
    ]);
    assert!(out.status.success(), "{out:?}");
    assert_eq!(
        read_tree(&out_dir.join("payload")),
        read_tree(&seed_dir.join("payload"))
    );
    assert!(
        server.requests().iter().any(|(_, range)| range.is_some()),
        "no ranged request reached the web seed"
    );
    drop(server);
}

/// The value of the first `<key>: ` line of `stdout`.
fn field<'a>(stdout: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}: ");
    stdout.lines().find_map(|l| l.strip_prefix(prefix.as_str()))
}

#[test]
fn create_then_info_round_trip() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("payload");
    std::fs::create_dir_all(data.join("sub")).expect("mkdir");
    std::fs::write(data.join("a.bin"), vec![7u8; 40_000]).expect("write");
    std::fs::write(data.join("sub/b.bin"), vec![9u8; 70_000]).expect("write");
    for (format, has_v1, has_v2) in [
        ("v1", true, false),
        ("v2", false, true),
        ("hybrid", true, true),
    ] {
        let out_file = tmp.path().join(format!("{format}.torrent"));
        let created = Command::new(BIN)
            .arg("create")
            .arg(&data)
            .arg("--out")
            .arg(&out_file)
            .args(["--format", format, "--piece-size", "32768"])
            .args(["--tracker", "http://tracker.invalid/announce"])
            .args(["--comment", "round trip"])
            .output()
            .expect("run create");
        assert!(created.status.success(), "{created:?}");
        let created_out = String::from_utf8_lossy(&created.stdout);
        assert_eq!(
            field(&created_out, "created"),
            Some(out_file.to_string_lossy().as_ref()),
            "{created_out}"
        );

        let m = Metainfo::from_bytes(&std::fs::read(&out_file).expect("read")).expect("parse");
        assert_eq!(m.info_hashes.v1.is_some(), has_v1, "{format}");
        assert_eq!(m.info_hashes.v2.is_some(), has_v2, "{format}");
        assert_eq!(
            m.trackers,
            vec![vec!["http://tracker.invalid/announce".to_owned()]]
        );

        let info = Command::new(BIN)
            .arg("info")
            .arg(&out_file)
            .output()
            .expect("run info");
        assert!(info.status.success(), "{info:?}");
        let stdout = String::from_utf8_lossy(&info.stdout);
        assert_eq!(field(&stdout, "version"), Some(format), "{stdout}");
        let wire = field(&stdout, "info hash").expect("info hash line");
        assert_eq!(wire.len(), 40, "{stdout}");
        assert!(wire.bytes().all(|b| b.is_ascii_hexdigit()), "{stdout}");
        if let Some(v1) = m.info_hashes.v1 {
            assert_eq!(wire, v1.to_string(), "{stdout}");
        }
        match m.info_hashes.v2 {
            Some(v2) => {
                let v2 = v2.to_string();
                assert_eq!(
                    field(&stdout, "info hash v2"),
                    Some(v2.as_str()),
                    "{stdout}"
                );
                if !has_v1 {
                    // A v2-only torrent's wire hash is its truncated v2 hash.
                    assert_eq!(wire, &v2[..40], "{stdout}");
                }
            }
            None => assert_eq!(field(&stdout, "info hash v2"), None, "{stdout}"),
        }
        // 110 000 bytes in 32 KiB pieces: 4 pieces for v1 (one stream), 2 + 3
        // for v2 and hybrid (pieces never span files).
        let pieces = if has_v2 { 5 } else { 4 };
        assert!(
            stdout.contains(&format!("pieces: {pieces} x 32.0 KiB")),
            "{format}: {stdout}"
        );
        // `create` prints the same hash lines as `info`.
        for key in ["info hash", "info hash v2", "version"] {
            assert_eq!(field(&created_out, key), field(&stdout, key), "{key}");
        }
    }
}

#[test]
fn create_bad_piece_size_is_a_clean_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("f.bin"), b"data").expect("write");
    let out = Command::new(BIN)
        .arg("create")
        .arg(tmp.path().join("f.bin"))
        .arg("--out")
        .arg(tmp.path().join("f.torrent"))
        .args(["--piece-size", "1000"])
        .output()
        .expect("run");
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("invalid piece length 1000"), "{stderr}");
    assert!(!tmp.path().join("f.torrent").exists());
}

#[test]
fn api_flag_serves_the_api() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (torrent_path, seed_dir, hash) = seed_fixture(tmp.path());
    let mut seed = Kill(
        Command::new(BIN)
            .arg("seed")
            .arg(&torrent_path)
            .arg("--data")
            .arg(&seed_dir)
            .args(["--port", "0", "--no-dht", "--api", "127.0.0.1:0"])
            .args(["--api-token", "t"])
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn seed"),
    );
    let stdout = seed.0.stdout.take().expect("stdout");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut lines = BufReader::new(stdout);
        let mut line = String::new();
        while lines.read_line(&mut line).map(|n| n > 0).unwrap_or(false) {
            if let Some(addr) = line.trim().strip_prefix("api: listening on ") {
                let _ = tx.send(addr.to_string());
            }
            // Keep draining so the seed never blocks on a full pipe.
            line.clear();
        }
    });
    let addr = rx
        .recv_timeout(Duration::from_secs(60))
        .expect("seed printed the api listening line");

    let mut conn = std::net::TcpStream::connect(&addr).expect("connect to the api");
    conn.set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    std::io::Write::write_all(
        &mut conn,
        format!(
            "GET /api/v1/torrents HTTP/1.1\r\nHost: {addr}\r\n\
             Authorization: Bearer t\r\nConnection: close\r\n\r\n"
        )
        .as_bytes(),
    )
    .expect("send request");
    let mut response = String::new();
    std::io::Read::read_to_string(&mut conn, &mut response).expect("read response");
    let (head, body) = response.split_once("\r\n\r\n").expect("a full response");
    assert!(head.starts_with("HTTP/1.1 200 "), "{response}");
    let body = body.trim();
    assert!(body.starts_with('[') && body.ends_with(']'), "{body}");
    assert_eq!(body.matches("\"info_hash_v1\":").count(), 1, "{body}");
    assert!(body.contains(&format!("\"id\":\"{hash}\"")), "{body}");

    let out = Command::new(BIN)
        .arg("seed")
        .arg(&torrent_path)
        .arg("--data")
        .arg(&seed_dir)
        .args(["--port", "0", "--no-dht", "--api", "127.0.0.1:0"])
        .output()
        .expect("run");
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("--api-token"), "{stderr}");
}
