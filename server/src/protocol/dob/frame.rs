use crate::protocol::dob::constants::{DEFAULT_MTU, FRAME_HEADER_SIZE, MAGIC_BYTES, SCHEMA_VERSION};

/// Errors returned by `DobFrameBuilder` when a message cannot be added.
#[derive(Debug, PartialEq)]
pub enum DobFrameError {
    ExceedsMtu { msg_size: usize, remaining: usize },
    MaxMessages,
}

/// Accumulates DZ-DoB binary application messages into a single frame buffer.
///
/// Usage mirrors `crate::protocol::frame::FrameBuilder`, but with DoB magic
/// (`0x4444`) and a `Reset Count` byte at offset 21 (TOB reserves this byte).
pub struct DobFrameBuilder {
    buf: Vec<u8>,
    channel_id: u8,
    sequence_number: u64,
    send_timestamp_ns: u64,
    reset_count: u8,
    mtu: usize,
    msg_count: u8,
}

impl DobFrameBuilder {
    pub fn new(
        channel_id: u8,
        sequence_number: u64,
        send_timestamp_ns: u64,
        reset_count: u8,
        mtu: u16,
    ) -> Self {
        let mtu = mtu as usize;
        let mut buf = Vec::with_capacity(mtu);
        buf.resize(FRAME_HEADER_SIZE, 0);
        Self {
            buf,
            channel_id,
            sequence_number,
            send_timestamp_ns,
            reset_count,
            mtu,
            msg_count: 0,
        }
    }

    #[must_use]
    pub fn remaining(&self) -> usize {
        self.mtu.saturating_sub(self.buf.len())
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.msg_count == 0
    }

    pub fn message_buffer(&mut self, size: usize) -> Result<&mut [u8], DobFrameError> {
        if self.msg_count == 255 {
            return Err(DobFrameError::MaxMessages);
        }
        if size > self.remaining() {
            return Err(DobFrameError::ExceedsMtu {
                msg_size: size,
                remaining: self.remaining(),
            });
        }
        let start = self.buf.len();
        self.buf.resize(start + size, 0);
        Ok(&mut self.buf[start..start + size])
    }

    pub fn commit_message(&mut self) {
        self.msg_count += 1;
    }

    pub fn finalize(&mut self) -> &[u8] {
        let frame_length = u16::try_from(self.buf.len()).unwrap_or(u16::MAX);
        self.buf[0..2].copy_from_slice(&MAGIC_BYTES);
        self.buf[2] = SCHEMA_VERSION;
        self.buf[3] = self.channel_id;
        self.buf[4..12].copy_from_slice(&self.sequence_number.to_le_bytes());
        self.buf[12..20].copy_from_slice(&self.send_timestamp_ns.to_le_bytes());
        self.buf[20] = self.msg_count;
        self.buf[21] = self.reset_count;
        self.buf[22..24].copy_from_slice(&frame_length.to_le_bytes());
        &self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_header_layout() {
        let mut fb = DobFrameBuilder::new(3, 42, 1_700_000_000_000_000_000, 7, DEFAULT_MTU);
        let buf = fb.message_buffer(8).unwrap();
        buf.copy_from_slice(&[0xAA; 8]);
        fb.commit_message();
        let frame = fb.finalize();

        assert_eq!(frame.len(), 32);
        assert_eq!(&frame[0..2], &[0x44, 0x44], "DoB magic");
        assert_eq!(frame[2], 1, "schema version");
        assert_eq!(frame[3], 3, "channel id");
        assert_eq!(u64::from_le_bytes(frame[4..12].try_into().unwrap()), 42, "seq");
        assert_eq!(u64::from_le_bytes(frame[12..20].try_into().unwrap()), 1_700_000_000_000_000_000);
        assert_eq!(frame[20], 1, "msg count");
        assert_eq!(frame[21], 7, "reset count");
        assert_eq!(u16::from_le_bytes(frame[22..24].try_into().unwrap()), 32, "frame length");
    }

    #[test]
    fn remaining_tracks_capacity() {
        let mut fb = DobFrameBuilder::new(0, 0, 0, 0, 100);
        assert_eq!(fb.remaining(), 76);
        let buf = fb.message_buffer(60).unwrap();
        buf.copy_from_slice(&[0; 60]);
        fb.commit_message();
        assert_eq!(fb.remaining(), 16);
    }

    #[test]
    fn exceeds_mtu_returns_error() {
        let mut fb = DobFrameBuilder::new(0, 0, 0, 0, 30);
        let result = fb.message_buffer(10);
        assert_eq!(result.unwrap_err(), DobFrameError::ExceedsMtu { msg_size: 10, remaining: 6 });
    }

    #[test]
    fn multiple_messages_in_frame() {
        let mut fb = DobFrameBuilder::new(0, 0, 0, 0, DEFAULT_MTU);
        for _ in 0..3 {
            let buf = fb.message_buffer(4).unwrap();
            buf.copy_from_slice(&[0xBB; 4]);
            fb.commit_message();
        }
        let frame = fb.finalize();
        assert_eq!(frame.len(), 24 + 3 * 4);
        assert_eq!(frame[20], 3);
    }

    #[test]
    fn max_messages_returns_error() {
        let mut fb = DobFrameBuilder::new(0, 0, 0, 0, u16::MAX);
        for _ in 0..255 {
            let _ = fb.message_buffer(4).unwrap();
            fb.commit_message();
        }
        assert_eq!(fb.message_buffer(4).unwrap_err(), DobFrameError::MaxMessages);
    }
}
