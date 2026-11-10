#![no_main]
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let Some((&first, mut rest)) = data.split_first() else {
        return;
    };
    let chunk_len = usize::from(first).max(1);
    let mut codec = fbt_wire::MessageCodec::new(1 << 20);
    let mut buf = BytesMut::new();
    while !rest.is_empty() {
        let n = chunk_len.min(rest.len());
        buf.extend_from_slice(&rest[..n]);
        rest = &rest[n..];
        loop {
            match codec.decode(&mut buf) {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => return,
            }
        }
    }
});
