//! Config structs round-trip through serde and tolerate missing fields.
#![cfg(feature = "serde")]

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use fbt_session::{
    ChokingAlgorithm, EncryptionPolicy, MixedMode, MoveMode, PeerClassLimits, Priority, ProxyKind,
    RateLimit, ReadAhead, RemoveMode, SeedChokingAlgorithm, Settings,
};

fn changed() -> Settings {
    let mut s = Settings::default();
    s.listen_addrs = vec![SocketAddr::from((Ipv4Addr::LOCALHOST, 7001))];
    s.max_peers_per_torrent = 11;
    s.max_peers_global = 22;
    s.max_peers_per_ip = 2;
    s.max_half_open = 3;
    s.unchoke_slots = 7;
    s.choking_algorithm = ChokingAlgorithm::RateBased;
    s.seed_choking_algorithm = SeedChokingAlgorithm::AntiLeech;
    s.optimistic_unchoke_slots = 2;
    s.optimistic_unchoke_interval = Duration::from_millis(31_500);
    s.seeding_piece_quota = 9;
    s.snub_timeout = Duration::from_secs(61);
    s.request_queue_time = Duration::from_millis(3_100);
    s.max_outstanding_requests = 501;
    s.max_incoming_requests = 251;
    s.connect_timeout = Duration::from_secs(11);
    s.handshake_timeout = Duration::from_secs(12);
    s.encryption = EncryptionPolicy::Forced;
    s.peer_idle_timeout = Duration::from_secs(121);
    s.keepalive_interval = Duration::from_secs(59);
    s.request_timeout = Duration::from_secs(58);
    s.peer_retry_delay = Duration::from_secs(57);
    s.rechoke_interval = Duration::from_secs(9);
    s.status_interval = Duration::from_millis(250);
    s.event_queue = 100;
    s.num_want = 7;
    s.tracker.user_agent = "other/1".to_owned();
    s.tracker.http_timeout = Duration::from_secs(5);
    s.tracker.udp_base_timeout = Duration::from_secs(6);
    s.tracker.udp_max_retries = 4;
    s.tracker.max_response_bytes = 1234;
    s.tracker_retry_base = Duration::from_secs(55);
    s.metadata_peer_grace = Duration::from_secs(29);
    s.max_metadata_size = 1 << 20;
    s.dht_enabled = false;
    s.dht_bootstrap_nodes = vec!["example.org:6881".to_owned()];
    s.dht_read_only = true;
    s.dht_state = Some(vec![1, 2, 3]);
    s.dht_announce_interval = Duration::from_secs(100);
    s.dht_query_timeout = Duration::from_secs(4);
    s.dht_query_rate_per_ip = 6;
    s.pex_enabled = false;
    s.pex_interval = Duration::from_secs(61);
    s.enable_outgoing_tcp = false;
    s.enable_incoming_tcp = false;
    s.enable_outgoing_utp = false;
    s.enable_incoming_utp = false;
    s.utp_connect_timeout = Duration::from_secs(4);
    s.enable_holepunch = false;
    s.enable_lsd = false;
    s.lsd_groups = vec![SocketAddr::from((Ipv4Addr::new(239, 1, 1, 1), 1))];
    s.lsd_announce_interval = Duration::from_secs(301);
    s.lsd_bind = Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 5)));
    s.enable_upnp = false;
    s.enable_natpmp = false;
    s.nat_gateway = Some(Ipv4Addr::new(10, 0, 0, 1));
    s.upnp_ssdp_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1900));
    s.natpmp_port = 5352;
    s.nat_retry_base = Duration::from_secs(61);
    s.nat_emulation = true;
    s.disk_threads = 3;
    s.max_active_checks = 2;
    s.cache_size = 1 << 20;
    s.cache_expiry = Duration::from_secs(31);
    let mut rl = RateLimit::default();
    rl.download = 100;
    rl.upload = 50;
    s.rate_limit = rl;
    let mut pc = PeerClassLimits::default();
    pc.tcp = rl;
    pc.utp.download = 7;
    pc.local.upload = 8;
    s.peer_class_limits = pc;
    s.rate_limit_local_peers = true;
    s.mixed_mode = MixedMode::PreferTcp;
    s.read_ahead = ReadAhead::new(1 << 20, Duration::from_secs(3), Some(500));
    // Milestone 5.
    s.proxy.kind = ProxyKind::Socks5;
    s.proxy.host = "proxy.example".to_owned();
    s.proxy.port = 1081;
    s.proxy.username = Some("alice".to_owned());
    s.proxy.password = Some("hunter2".to_owned());
    s.proxy.proxy_peers = false;
    s.proxy.proxy_trackers = false;
    s.proxy.proxy_hostnames = false;
    s.queue.active_downloads = Some(2);
    s.queue.active_seeds = None;
    s.queue.active_limit = Some(9);
    s.queue.dont_count_slow_torrents = false;
    s.queue.inactive_down_rate = 1_234;
    s.queue.inactive_up_rate = 4_321;
    s.queue.startup_grace = Duration::from_secs(77);
    s.queue.interval = Duration::from_secs(13);
    s.seed_ratio_limit = Some(1.5);
    s.seed_time_limit = Some(Duration::from_secs(3_600));
    s
}

#[test]
fn settings_round_trip_through_json() {
    let s = changed();
    let json = serde_json::to_string(&s).unwrap();
    let back: Settings = serde_json::from_str(&json).unwrap();
    assert_eq!(format!("{s:?}"), format!("{back:?}"));
    assert_ne!(format!("{s:?}"), format!("{:?}", Settings::default()));
    // `Debug` hides the password: it is written in clear and read back.
    assert!(json.contains("hunter2"), "{json}");
    assert_eq!(back.proxy, s.proxy);
    assert_eq!(back.queue, s.queue);
    assert_eq!(back.seed_ratio_limit, Some(1.5));
}

#[test]
fn test_knobs_are_not_serialised() {
    let mut s = Settings::default();
    s.per_ip_exempt_loopback = !s.per_ip_exempt_loopback;
    s.ban_endpoints = !s.ban_endpoints;
    let json = serde_json::to_string(&s).unwrap();
    assert!(!json.contains("per_ip_exempt_loopback"), "{json}");
    assert!(!json.contains("ban_endpoints"), "{json}");
    let back: Settings = serde_json::from_str(&json).unwrap();
    let d = Settings::default();
    assert_eq!(back.per_ip_exempt_loopback, d.per_ip_exempt_loopback);
    assert_eq!(back.ban_endpoints, d.ban_endpoints);
    // A saved file that has them is still read; they keep their defaults.
    let back: Settings =
        serde_json::from_str(r#"{"per_ip_exempt_loopback": false, "ban_endpoints": true}"#)
            .unwrap();
    assert_eq!(back.per_ip_exempt_loopback, d.per_ip_exempt_loopback);
    assert_eq!(back.ban_endpoints, d.ban_endpoints);
}

#[test]
fn missing_fields_take_defaults() {
    let s: Settings = serde_json::from_str("{}").unwrap();
    assert_eq!(format!("{s:?}"), format!("{:?}", Settings::default()));
    let s: Settings = serde_json::from_str(r#"{"num_want": 9}"#).unwrap();
    assert_eq!(s.num_want, 9);
    assert_eq!(s.max_peers_global, Settings::default().max_peers_global);
    let s: Settings = serde_json::from_str(r#"{"tracker": {}, "rate_limit": {}}"#).unwrap();
    assert_eq!(format!("{s:?}"), format!("{:?}", Settings::default()));
}

#[test]
fn read_ahead_and_priority_round_trip() {
    let ra = ReadAhead::new(5, Duration::from_millis(1500), Some(9));
    let back: ReadAhead = serde_json::from_str(&serde_json::to_string(&ra).unwrap()).unwrap();
    assert_eq!(ra, back);
    let none: ReadAhead = serde_json::from_str("{}").unwrap();
    assert_eq!(none, ReadAhead::default());
    for p in [
        Priority::Skip,
        Priority::Low,
        Priority::Normal,
        Priority::High,
    ] {
        let back: Priority = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        assert_eq!(p, back);
    }
    for m in [RemoveMode::KeepData, RemoveMode::DeleteData] {
        let back: RemoveMode = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(m, back);
    }
    for m in [
        MoveMode::FailIfExists,
        MoveMode::ReplaceExisting,
        MoveMode::KeepExisting,
    ] {
        let back: MoveMode = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(m, back);
    }
}
