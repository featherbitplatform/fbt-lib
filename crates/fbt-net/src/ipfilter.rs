//! IP filter: block and allow ranges of addresses, the last rule wins.
//!
//! Each address family keeps its rules as disjoint ranges in a
//! `BTreeMap` from the range's first address to its last and its verdict,
//! so a lookup is one `O(log n)` search. A new rule overwrites what it
//! covers, and neighbouring ranges with the same verdict merge, so the map
//! never holds more ranges than it needs. IPv4-mapped IPv6 addresses
//! (`::ffff:a.b.c.d`) are IPv4 addresses, in rules and lookups alike.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::ops::RangeInclusive;

use tracing::{debug, warn};

/// Most disjoint ranges an [`IpFilter`] holds, both families together. A
/// rule that could take it past this is ignored (and logged once).
pub const MAX_RANGES: usize = 1_000_000;

/// Ranges of addresses that are blocked or explicitly allowed; everything
/// else is allowed. Rules apply in order and the last one covering an
/// address decides. The default filter allows everything.
///
/// IPv4 and IPv6 rules are separate: an IPv6 range never blocks an IPv4
/// address, and an IPv4-mapped address (`::ffff:a.b.c.d`) is the IPv4
/// address `a.b.c.d`, whether in a rule or looked up. A rule whose ends are
/// of different families, or whose start is above its end, is ignored.
///
/// With the `serde` feature it serialises as its list of disjoint rules,
/// each `{ start, end, blocked }`, and is read back through
/// [`block`](Self::block) and [`allow`](Self::allow).
#[derive(Debug, Clone, Default)]
pub struct IpFilter {
    v4: Ranges,
    v6: Ranges,
    /// A rule was ignored at [`MAX_RANGES`] and that was logged.
    capped: bool,
}

impl PartialEq for IpFilter {
    fn eq(&self, other: &Self) -> bool {
        self.v4 == other.v4 && self.v6 == other.v6
    }
}

impl Eq for IpFilter {}

impl IpFilter {
    /// Blocks every address in `range`.
    pub fn block(&mut self, range: RangeInclusive<IpAddr>) {
        self.rule(range, true);
    }

    /// Allows every address in `range`, overriding earlier blocks.
    pub fn allow(&mut self, range: RangeInclusive<IpAddr>) {
        self.rule(range, false);
    }

    /// Whether `ip` is blocked.
    pub fn is_blocked(&self, ip: IpAddr) -> bool {
        match ip.to_canonical() {
            IpAddr::V4(v4) => self.v4.lookup(u128::from(u32::from(v4))),
            IpAddr::V6(v6) => self.v6.lookup(u128::from(v6)),
        }
    }

    /// The number of disjoint ranges held, blocked or allowed.
    pub fn len(&self) -> usize {
        self.v4.0.len().saturating_add(self.v6.0.len())
    }

    /// No rule is held: everything is allowed.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The disjoint ranges with their verdict (true: blocked), IPv4 first,
    /// each family in ascending order. Applying them in any order to an
    /// empty filter rebuilds this one.
    pub fn rules(&self) -> Vec<(RangeInclusive<IpAddr>, bool)> {
        let v4 = self
            .v4
            .0
            .iter()
            .map(|(&s, &(e, b))| (v4_at(s)..=v4_at(e), b));
        let v6 = self.v6.0.iter().map(|(&s, &(e, b))| {
            (
                IpAddr::V6(Ipv6Addr::from(s))..=IpAddr::V6(Ipv6Addr::from(e)),
                b,
            )
        });
        v4.chain(v6).collect()
    }

    fn rule(&mut self, range: RangeInclusive<IpAddr>, blocked: bool) {
        let Some((v4, start, end)) = bounds(&range) else {
            debug!(?range, "ignoring a reversed or mixed-family IP filter rule");
            return;
        };
        // One rule adds at most two ranges (itself and the far end of a
        // range it splits).
        if self.len().saturating_add(2) > MAX_RANGES {
            if !self.capped {
                self.capped = true;
                warn!(
                    max = MAX_RANGES,
                    "IP filter full; further rules are ignored"
                );
            }
            return;
        }
        let ranges = if v4 { &mut self.v4 } else { &mut self.v6 };
        ranges.set(start, end, blocked);
    }
}

/// `range` as `(is_v4, start, end)` keys, IPv4-mapped ends as IPv4; `None`
/// for ends of different families or a start above the end.
fn bounds(range: &RangeInclusive<IpAddr>) -> Option<(bool, u128, u128)> {
    let (a, b) = (*range.start(), *range.end());
    let (a, b) = match (a.to_canonical(), b.to_canonical()) {
        (x @ IpAddr::V4(_), y @ IpAddr::V4(_)) => (x, y),
        _ => (a, b),
    };
    let keys = match (a, b) {
        (IpAddr::V4(x), IpAddr::V4(y)) => {
            (true, u128::from(u32::from(x)), u128::from(u32::from(y)))
        }
        (IpAddr::V6(x), IpAddr::V6(y)) => (false, u128::from(x), u128::from(y)),
        _ => return None,
    };
    (keys.1 <= keys.2).then_some(keys)
}

/// The IPv4 address of a key (always in range: IPv4 keys come from `u32`s).
fn v4_at(key: u128) -> IpAddr {
    IpAddr::V4(Ipv4Addr::from(u32::try_from(key).unwrap_or(u32::MAX)))
}

/// One family's rules: disjoint ranges, first address to (last address,
/// blocked), never two adjacent ones with the same verdict.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Ranges(BTreeMap<u128, (u128, bool)>);

impl Ranges {
    fn lookup(&self, key: u128) -> bool {
        self.0
            .range(..=key)
            .next_back()
            .is_some_and(|(_, &(end, blocked))| key <= end && blocked)
    }

    /// Sets `start..=end` to `blocked`, overwriting what it covers.
    fn set(&mut self, start: u128, end: u128, blocked: bool) {
        // A range starting before `start` that reaches into it keeps its
        // part before `start`, and its part after `end` if it has one.
        if let Some((&s0, &(e0, b0))) = self.0.range(..start).next_back()
            && e0 >= start
        {
            self.0.insert(s0, (start.saturating_sub(1), b0));
            if e0 > end {
                self.0.insert(end.saturating_add(1), (e0, b0));
            }
        }
        // Ranges starting inside go, but for a part after `end`.
        let inside: Vec<u128> = self.0.range(start..=end).map(|(&s, _)| s).collect();
        for s in inside {
            if let Some((e, b)) = self.0.remove(&s)
                && e > end
            {
                self.0.insert(end.saturating_add(1), (e, b));
            }
        }
        let mut start = start;
        let mut end = end;
        // Merge with the neighbours of the same verdict.
        if let Some(before) = start.checked_sub(1)
            && let Some((&s0, &(e0, b0))) = self.0.range(..start).next_back()
            && e0 == before
            && b0 == blocked
        {
            start = s0;
        }
        if let Some(after) = end.checked_add(1)
            && let Some(&(e1, b1)) = self.0.get(&after)
            && b1 == blocked
        {
            self.0.remove(&after);
            end = e1;
        }
        self.0.insert(start, (end, blocked));
    }
}

#[cfg(feature = "serde")]
mod serde_impl {
    use std::fmt;
    use std::net::IpAddr;

    use serde::de::{SeqAccess, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::IpFilter;

    /// One rule as it is persisted.
    #[derive(Serialize, Deserialize)]
    struct Rule {
        start: IpAddr,
        end: IpAddr,
        blocked: bool,
    }

    impl Serialize for IpFilter {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            let rules = self.rules();
            let mut seq = s.serialize_seq(Some(rules.len()))?;
            for (range, blocked) in rules {
                seq.serialize_element(&Rule {
                    start: *range.start(),
                    end: *range.end(),
                    blocked,
                })?;
            }
            seq.end()
        }
    }

    impl<'de> Deserialize<'de> for IpFilter {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            d.deserialize_seq(RulesVisitor)
        }
    }

    /// Applies each rule as it is read, so nothing but the filter itself
    /// is allocated.
    struct RulesVisitor;

    impl<'de> Visitor<'de> for RulesVisitor {
        type Value = IpFilter;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a list of IP filter rules")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<IpFilter, A::Error> {
            let mut filter = IpFilter::default();
            while let Some(rule) = seq.next_element::<Rule>()? {
                let range = rule.start..=rule.end;
                if rule.blocked {
                    filter.block(range);
                } else {
                    filter.allow(range);
                }
            }
            Ok(filter)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("address")
    }

    fn r(a: &str, b: &str) -> std::ops::RangeInclusive<IpAddr> {
        ip(a)..=ip(b)
    }

    #[test]
    fn last_rule_wins_and_ranges_merge() {
        let mut f = IpFilter::default();
        assert!(f.is_empty());
        assert!(!f.is_blocked(ip("10.0.0.1")));
        f.block(r("10.0.0.0", "10.0.0.255"));
        f.block(r("10.0.1.0", "10.0.1.255"));
        // Adjacent ranges with the same verdict are one.
        assert_eq!(f.len(), 1);
        assert!(f.is_blocked(ip("10.0.0.0")));
        assert!(f.is_blocked(ip("10.0.1.255")));
        assert!(!f.is_blocked(ip("10.0.2.0")));
        assert!(!f.is_blocked(ip("9.255.255.255")));
        // A later allow punches a hole.
        f.allow(r("10.0.0.128", "10.0.0.130"));
        assert_eq!(f.len(), 3);
        assert!(f.is_blocked(ip("10.0.0.127")));
        assert!(!f.is_blocked(ip("10.0.0.128")));
        assert!(!f.is_blocked(ip("10.0.0.130")));
        assert!(f.is_blocked(ip("10.0.0.131")));
        assert_eq!(
            f.rules(),
            vec![
                (r("10.0.0.0", "10.0.0.127"), true),
                (r("10.0.0.128", "10.0.0.130"), false),
                (r("10.0.0.131", "10.0.1.255"), true),
            ]
        );
        // A later block over everything closes it again.
        f.block(r("10.0.0.0", "10.0.1.255"));
        assert_eq!(f.len(), 1);
        assert!(f.is_blocked(ip("10.0.0.129")));
        // Overlapping on both sides merges into one range.
        f.block(r("9.0.0.0", "10.0.0.5"));
        f.block(r("10.0.1.200", "11.0.0.0"));
        assert_eq!(f.rules(), vec![(r("9.0.0.0", "11.0.0.0"), true)]);
        // The whole space, then an allow at each end.
        let mut all = IpFilter::default();
        all.block(r("0.0.0.0", "255.255.255.255"));
        all.allow(r("0.0.0.0", "0.0.0.0"));
        all.allow(r("255.255.255.255", "255.255.255.255"));
        assert!(!all.is_blocked(ip("0.0.0.0")));
        assert!(all.is_blocked(ip("0.0.0.1")));
        assert!(all.is_blocked(ip("255.255.255.254")));
        assert!(!all.is_blocked(ip("255.255.255.255")));
        assert_eq!(all.len(), 3);
        let mut v6 = IpFilter::default();
        v6.block(r("::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
        assert!(v6.is_blocked(ip("2001:db8::1")));
        assert_eq!(v6.len(), 1);
    }

    #[test]
    fn v4_mapped_is_canonical() {
        let mut f = IpFilter::default();
        f.block(r("1.2.3.4", "1.2.3.4"));
        assert!(f.is_blocked(ip("::ffff:1.2.3.4")));
        // A rule written in the mapped form is an IPv4 rule.
        f.block(r("::ffff:5.6.7.8", "::ffff:5.6.7.9"));
        assert!(f.is_blocked(ip("5.6.7.9")));
        assert!(f.is_blocked(ip("::ffff:5.6.7.8")));
        assert!(!f.is_blocked(ip("5.6.7.10")));
        assert_eq!(
            f.rules(),
            vec![
                (r("1.2.3.4", "1.2.3.4"), true),
                (r("5.6.7.8", "5.6.7.9"), true),
            ]
        );
    }

    #[test]
    fn families_are_separate() {
        let mut f = IpFilter::default();
        f.block(r("0.0.0.0", "255.255.255.255"));
        assert!(!f.is_blocked(ip("2001:db8::1")));
        assert!(!f.is_blocked(ip("::1")));
        let mut g = IpFilter::default();
        g.block(r("::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
        assert!(!g.is_blocked(ip("10.0.0.1")));
        // A range from one family to the other is ignored.
        let mut h = IpFilter::default();
        h.block(r("10.0.0.1", "2001:db8::1"));
        h.allow(r("::1", "10.0.0.1"));
        assert!(h.is_empty());
        assert_eq!(h, IpFilter::default());
    }

    #[test]
    fn reversed_range_is_ignored() {
        let mut f = IpFilter::default();
        f.block(r("10.0.0.9", "10.0.0.1"));
        f.block(r("2001:db8::9", "2001:db8::1"));
        assert!(f.is_empty());
        assert!(!f.is_blocked(ip("10.0.0.5")));
        assert!(!f.is_blocked(ip("2001:db8::5")));
    }

    /// A rule over a small corner of each family: the IPv4 addresses
    /// `10.0.0.0..=10.0.0.63` and the IPv6 ones `2001:db8::..=2001:db8::3f`.
    fn small(v6: bool, n: u8) -> IpAddr {
        if v6 {
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, u16::from(n)))
        } else {
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, n))
        }
    }

    proptest! {
        #[test]
        fn matches_a_naive_rule_list(
            rules in proptest::collection::vec(
                (any::<bool>(), 0u8..64, 0u8..64, any::<bool>()),
                0..40,
            )
        ) {
            let mut f = IpFilter::default();
            let mut naive: Vec<(bool, u8, u8, bool)> = Vec::new();
            for &(v6, a, b, blocked) in &rules {
                let range = small(v6, a)..=small(v6, b);
                if blocked {
                    f.block(range);
                } else {
                    f.allow(range);
                }
                if a <= b {
                    naive.push((v6, a, b, blocked));
                }
            }
            for v6 in [false, true] {
                for n in 0u8..66 {
                    let want = naive
                        .iter()
                        .rev()
                        .find(|(f6, a, b, _)| *f6 == v6 && (*a..=*b).contains(&n))
                        .is_some_and(|(_, _, _, blocked)| *blocked);
                    prop_assert_eq!(f.is_blocked(small(v6, n)), want, "v6 {} n {}", v6, n);
                }
            }
            // Ranges are disjoint, sorted and never adjacent with one verdict.
            let rules = f.rules();
            prop_assert_eq!(rules.len(), f.len());
            for w in rules.windows(2) {
                let (Some((a, va)), Some((b, vb))) = (w.first(), w.get(1)) else {
                    continue;
                };
                prop_assert!(a.end() < b.start());
                if a.end().is_ipv4() == b.start().is_ipv4() && va == vb {
                    let next = match *a.end() {
                        IpAddr::V4(x) => IpAddr::V4(Ipv4Addr::from(u32::from(x) + 1)),
                        IpAddr::V6(x) => IpAddr::V6(Ipv6Addr::from(u128::from(x) + 1)),
                    };
                    prop_assert!(next != *b.start(), "adjacent ranges not merged");
                }
            }
            // Replaying the rules rebuilds the same filter.
            let mut back = IpFilter::default();
            for (range, blocked) in rules {
                if blocked { back.block(range) } else { back.allow(range) }
            }
            prop_assert_eq!(back, f);
        }
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_is_a_rule_list() {
        let mut f = IpFilter::default();
        f.block(r("10.0.0.0", "10.0.0.255"));
        f.allow(r("10.0.0.7", "10.0.0.7"));
        f.block(r("2001:db8::", "2001:db8::ff"));
        let json = serde_json::to_value(&f).expect("serialise");
        assert_eq!(
            json,
            serde_json::json!([
                {"start": "10.0.0.0", "end": "10.0.0.6", "blocked": true},
                {"start": "10.0.0.7", "end": "10.0.0.7", "blocked": false},
                {"start": "10.0.0.8", "end": "10.0.0.255", "blocked": true},
                {"start": "2001:db8::", "end": "2001:db8::ff", "blocked": true},
            ])
        );
        let back: IpFilter = serde_json::from_value(json).expect("deserialise");
        assert_eq!(back, f);
        // Read back through `block`/`allow`: overlapping rules apply in
        // order, and a reversed or mixed one is ignored.
        let g: IpFilter = serde_json::from_str(
            r#"[
                {"start": "10.0.0.0", "end": "10.0.0.9", "blocked": true},
                {"start": "10.0.0.5", "end": "10.0.0.9", "blocked": false},
                {"start": "10.0.0.9", "end": "10.0.0.1", "blocked": true},
                {"start": "10.0.0.1", "end": "::1", "blocked": true}
            ]"#,
        )
        .expect("deserialise");
        assert_eq!(
            g.rules(),
            vec![
                (r("10.0.0.0", "10.0.0.4"), true),
                (r("10.0.0.5", "10.0.0.9"), false),
            ]
        );
        let empty: IpFilter = serde_json::from_str("[]").expect("deserialise");
        assert!(empty.is_empty());
    }
}
