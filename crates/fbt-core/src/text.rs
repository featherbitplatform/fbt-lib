//! Display-safe rendering of text that came from a peer, tracker or `.torrent`.
//!
//! Remote strings end up in front of a user: in a peer list, an event, an error
//! message, or as a file name on disk. Hostile ones spoof what is seen. A bidi
//! override (U+202E) reverses the text after it, so a name that ends `.png`
//! displays as if it ended `.jpg`; a zero-width character hides what follows it;
//! a control character moves the cursor or ends the line; a lone combining mark
//! lands on whatever came before it; and an unbounded one grows whatever holds
//! it. [`display_safe`] drops all of that and cuts the result to a length the
//! caller chooses, so remote text can neither spoof a display nor allocate
//! without bound.
//!
//! It is applied where remote text reaches users: names and comments in
//! `.torrent` files and magnet links (and so every path component written to
//! disk), tracker failure and warning text, web-seed errors and peer client
//! names.

// General category M (marks) comes from the same Unicode table `fbt-metainfo`
// already uses to NFC-normalise its collision keys; Cf is listed by hand below.
use unicode_normalization::char::is_combining_mark;

/// Longest string [`display_safe`] keeps, in characters: the cap on remote
/// messages and names that reach a user.
pub const MAX_DISPLAY_CHARS: usize = 256;

/// A Unicode format character (general category Cf): soft hyphen, bidi marks,
/// embeddings, overrides and isolates, zero-width characters, BOM, interlinear
/// annotation and tag characters, and the rest of Cf.
fn is_format(c: char) -> bool {
    matches!(
        u32::from(c),
        0xAD | 0x600..=0x605
            | 0x61C
            | 0x6DD
            | 0x70F
            | 0x890..=0x891
            | 0x8E2
            | 0x180E
            | 0x200B..=0x200F
            | 0x202A..=0x202E
            | 0x2060..=0x2064
            | 0x2066..=0x206F
            | 0xFEFF
            | 0xFFF9..=0xFFFB
            | 0x110BD
            | 0x110CD
            | 0x13430..=0x1343F
            | 0x1BCA0..=0x1BCA3
            | 0x1D173..=0x1D17A
            | 0xE0001
            | 0xE0020..=0xE007F
    )
}

/// `s` as a user should see it: Unicode control (Cc) and format (Cf) characters
/// dropped, every run of whitespace replaced by one space, surrounding
/// whitespace trimmed, combining marks that have no kept character before them
/// dropped, and the result cut to `max_chars` characters on a character
/// boundary. Nothing usable left gives the empty string.
///
/// Control characters that Unicode classes as whitespace (`\t`, `\n`, `\r`,
/// `U+0085`) count as whitespace here, so they separate words rather than
/// disappear inside one. Text is otherwise left as it came: no case change and
/// no normalisation, so a name that survives the filter keeps its exact bytes.
pub fn display_safe(s: &str, max_chars: usize) -> String {
    // At most four bytes per kept character and never more than `s`: neither
    // the cap nor the input can make this allocate without bound.
    let mut out = String::with_capacity(max_chars.saturating_mul(4).min(s.len()));
    // Kept characters, spaces included: the cap counts characters.
    let mut kept = 0usize;
    // A separator seen since the last kept character. It becomes one space, or
    // nothing at all while nothing is kept yet, which is what trims the front;
    // one never goes on the end, because a run at the tail stays pending.
    let mut space = false;
    for c in s.chars() {
        if c.is_whitespace() {
            space = true;
            continue;
        }
        // Cc (the whitespace ones went above) and Cf are invisible: gone. A mark
        // with nothing kept before it has no base to sit on, so it goes too.
        if is_format(c) || c.is_control() || (kept == 0 && is_combining_mark(c)) {
            continue;
        }
        if space && kept > 0 && kept < max_chars {
            out.push(' ');
            kept += 1;
            space = false;
        }
        if kept == max_chars {
            break;
        }
        out.push(c);
        kept += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The table every caller relies on.
    #[test]
    fn display_safe_table() {
        for (input, want) in [
            ("ok", "ok"),
            ("a\u{202E}b", "ab"),
            ("a\u{200B}\u{FEFF}b", "ab"),
            ("a\nb\tc", "a b c"),
            ("\u{301}x", "x"),
            ("  a   b  ", "a b"),
            ("\u{85}x", "x"), // C1 control (NEL): a separator, trimmed at the front
            ("", ""),
        ] {
            assert_eq!(display_safe(input, MAX_DISPLAY_CHARS), want, "{input:?}");
        }
        // 10 000 characters cut to `max_chars`, on a character boundary.
        let long = "é".repeat(10_000);
        let out = display_safe(&long, MAX_DISPLAY_CHARS);
        assert_eq!(out.chars().count(), MAX_DISPLAY_CHARS);
        assert!(out.chars().all(|c| c == 'é'));
    }

    /// Control and format characters go; other text, including non-ASCII, stays.
    #[test]
    fn control_and_format_characters_are_dropped() {
        let out = display_safe("a\u{1b}[31m\u{7f}b", MAX_DISPLAY_CHARS);
        assert_eq!(out, "a[31mb");
        for cp in [
            0xADu32, 0x600, 0x61C, 0x6DD, 0x70F, 0x890, 0x8E2, 0x180E, 0x200B, 0x200D, 0x202A,
            0x202E, 0x2060, 0x2066, 0x2069, 0xFEFF, 0xFFF9, 0x110BD, 0x110CD, 0x1343F, 0x1BCA0,
            0x1D173, 0xE0001, 0xE0020, 0xE007F,
        ] {
            let c = char::from_u32(cp).unwrap();
            assert!(is_format(c), "{c:?}");
            assert_eq!(display_safe(&format!("x{c}y"), MAX_DISPLAY_CHARS), "xy");
        }
        for cp in [0u32, 1, 0x1b, 0x1f, 0x7f, 0x80, 0x9f] {
            let c = char::from_u32(cp).unwrap();
            assert!(!c.is_whitespace(), "{c:?} is a control character");
            assert_eq!(display_safe(&format!("x{c}y"), MAX_DISPLAY_CHARS), "xy");
        }
    }

    /// Every separator, whatever its width or script, becomes one space, and
    /// the ends are trimmed.
    #[test]
    fn whitespace_collapses_to_one_space() {
        for cp in [
            0x9u32, 0xA, 0xB, 0xC, 0xD, 0x85, 0x20, 0xA0, 0x1680, 0x2000, 0x2028, 0x2029, 0x202F,
            0x205F, 0x3000,
        ] {
            let c = char::from_u32(cp).unwrap();
            assert_eq!(
                display_safe(&format!("a{c}b"), MAX_DISPLAY_CHARS),
                "a b",
                "{c:?}"
            );
        }
        assert_eq!(display_safe("\u{a0}x\u{3000}", MAX_DISPLAY_CHARS), "x");
        assert_eq!(display_safe("a \t\n\r b", MAX_DISPLAY_CHARS), "a b");
    }

    /// A mark stays when something precedes it and goes when nothing does.
    #[test]
    fn marks_need_a_base() {
        // Composed and decomposed `é` both stay: neither is a lone mark.
        assert_eq!(display_safe("éx", MAX_DISPLAY_CHARS), "éx");
        assert_eq!(display_safe("e\u{301}x", MAX_DISPLAY_CHARS), "e\u{301}x");
        // A mark with nothing before it, and one whose base was dropped.
        assert_eq!(display_safe("\u{301}\u{302}x", MAX_DISPLAY_CHARS), "x");
        assert_eq!(display_safe("\u{202E}\u{301}x", MAX_DISPLAY_CHARS), "x");
        // Marks elsewhere than the start are left alone.
        assert_eq!(display_safe("x\u{301}y", MAX_DISPLAY_CHARS), "x\u{301}y");
    }

    /// The cap counts characters, never splits one, and bounds the output.
    #[test]
    fn the_cap_is_a_character_boundary() {
        let out = display_safe("日本語のテキスト", 4);
        assert_eq!(out.chars().count(), 4);
        assert_eq!(out, "日本語の");
        // A space run does not push the result past the cap.
        let out = display_safe("a b c d e", 3);
        assert_eq!(out, "a b");
        // Nothing is unlimited: an uncapped call still gives back at most `s`.
        let long = "x".repeat(10_000);
        assert_eq!(display_safe(&long, usize::MAX), long);
    }

    /// A name a peer sends, as the session shows it.
    #[test]
    fn a_spoofed_client_name_is_flattened() {
        assert_eq!(display_safe("qB\u{202E}4.6\u{200B}\nx", 64), "qB4.6 x");
    }
}
