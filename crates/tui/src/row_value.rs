use onetui_core::value::ValueFormat;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use crate::value::{BYTE_CHUNK, Prepared, byte_chunk};

/// A viewport over one retained value, never a second full hex/binary allocation.
// ponytail: wrapping scans the bounded value; cache line offsets if profiling shows input lag.
pub(crate) fn viewport(
    prepared: &Prepared,
    bytes: &[u8],
    width: usize,
    wrap: bool,
    scroll: usize,
    height: usize,
) -> (String, usize) {
    let mut visible = String::new();
    let mut total = 0;
    let mut emit = |line: &str| {
        if total >= scroll && total - scroll < height {
            if total > scroll {
                visible.push('\n');
            }
            visible.push_str(line);
        }
        total += 1;
    };
    let mut visit = |source: &str| {
        for line in source.split('\n') {
            if !wrap {
                emit(line);
                continue;
            }
            let mut start = 0;
            let mut offset = 0;
            let mut used = 0;
            for word in line.split_word_bounds() {
                if used > 0 && used + word.width() > width {
                    emit(&line[start..offset]);
                    start = offset;
                    used = 0;
                }
                for grapheme in word.graphemes(true) {
                    if used > 0 && used + grapheme.width() > width {
                        emit(&line[start..offset]);
                        start = offset;
                        used = 0;
                    }
                    offset += grapheme.len();
                    used += grapheme.width();
                }
            }
            emit(&line[start..]);
        }
    };
    if matches!(prepared.format, ValueFormat::Hex | ValueFormat::Binary) {
        for chunk in 0..bytes.len().div_ceil(BYTE_CHUNK).max(1) {
            visit(&byte_chunk(bytes, prepared.format, chunk));
        }
    } else {
        visit(&prepared.text);
    }
    (visible, total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use onetui_core::{Value, value::DisplayOptions};

    #[test]
    fn viewport_reaches_the_entire_value_including_bytes_and_unicode() {
        for value in [
            Value::Text("first\n".repeat(70_000) + "last 🌊東京"),
            Value::Bytes((0..=255).cycle().take(1024).collect()),
        ] {
            for format in [ValueFormat::Auto, ValueFormat::Hex, ValueFormat::Binary] {
                let prepared = crate::value::prepare(
                    Some(&value),
                    DisplayOptions {
                        format,
                        ..DisplayOptions::default()
                    },
                    false,
                );
                for wrap in [false, true] {
                    let (_, count) = viewport(&prepared, value.bytes(), 15, wrap, 0, 0);
                    let (last, same_count) =
                        viewport(&prepared, value.bytes(), 15, wrap, count - 1, 1);
                    assert_eq!(count, same_count);
                    assert!(!last.is_empty());
                    assert!(!last.contains('…'));
                    let (past, _) = viewport(&prepared, value.bytes(), 15, wrap, count, 1);
                    assert!(past.is_empty());
                }
            }
        }
        let value = Value::Text("a🌊東京e\u{301}\n\nlast".into());
        let prepared = crate::value::prepare(Some(&value), DisplayOptions::default(), false);
        let (wrapped, count) = viewport(&prepared, value.bytes(), 2, true, 0, usize::MAX);
        assert_eq!(wrapped.replace('\n', ""), "a🌊東京e\u{301}last");
        assert!(count > 3 && wrapped.contains("e\u{301}"));
    }
}
