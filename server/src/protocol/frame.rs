use crate::protocol::constants::*;

/// Errors returned by FrameBuilder when a message cannot be added.
#[derive(Debug, PartialEq)]
pub enum FrameError {
    /// Adding this message would exceed the configured MTU.
    ExceedsMtu { msg_size: usize, remaining: usize },
    /// Frame already has 255 messages (max for u8 msg_count).
    MaxMessages,
}

/// Accumulates binary application messages into a single frame buffer.
///
/// Usage:
/// 1. Create with `new(channel_id, sequence_number, send_timestamp_ns, mtu)`
/// 2. Call `remaining()` to check capacity, then write message bytes via `message_buffer()`
/// 3. Call `commit_message()` after writing to advance the cursor
/// 4. Call `finalize()` to write the frame header and get the complete frame bytes
pub struct FrameBuilder {
    buf: Vec<u8>,
    channel_id: u8,
    sequence_number: u64,
    send_timestamp_ns: u64,
    mtu: usize,
    msg_count: u8,
}

impl FrameBuilder {
    /// Creates a new frame builder. Reserves space for the frame header.
    pub fn new(channel_id: u8, sequence_number: u64, send_timestamp_ns: u64, mtu: u16) -> Self {
        let mtu = mtu as usize;
        let mut buf = Vec::with_capacity(mtu);
        // Reserve frame header space — will be written in finalize()
        buf.resize(FRAME_HEADER_SIZE, 0);
        Self {
            buf,
            channel_id,
            sequence_number,
            send_timestamp_ns,
            mtu,
            msg_count: 0,
        }
    }

    /// Returns the number of bytes available for more messages.
    pub fn remaining(&self) -> usize {
        self.mtu.saturating_sub(self.buf.len())
    }

    /// Returns true if no messages have been added yet.
    pub fn is_empty(&self) -> bool {
        self.msg_count == 0
    }

    /// Returns a mutable slice of exactly `size` bytes at the end of the buffer
    /// for the caller to write a message into. Returns `Err` if the message
    /// won't fit or if 255 messages have already been added.
    ///
    /// After writing, the caller MUST call `commit_message()`.
    pub fn message_buffer(&mut self, size: usize) -> Result<&mut [u8], FrameError> {
        if self.msg_count == 255 {
            return Err(FrameError::MaxMessages);
        }
        if size > self.remaining() {
            return Err(FrameError::ExceedsMtu {
                msg_size: size,
                remaining: self.remaining(),
            });
        }
        let start = self.buf.len();
        self.buf.resize(start + size, 0);
        Ok(&mut self.buf[start..start + size])
    }

    /// Commits the most recently allocated message, incrementing the message count.
    pub fn commit_message(&mut self) {
        self.msg_count += 1;
    }

    /// Writes the frame header into the reserved space and returns the complete frame.
    pub fn finalize(&mut self) -> &[u8] {
        let frame_length = self.buf.len() as u16;

        // Magic (0x445A LE → bytes [0x5A, 0x44])
        self.buf[0..2].copy_from_slice(&MAGIC_BYTES);
        // Schema Version
        self.buf[2] = SCHEMA_VERSION;
        // Channel ID
        self.buf[3] = self.channel_id;
        // Sequence Number
        self.buf[4..12].copy_from_slice(&self.sequence_number.to_le_bytes());
        // Send Timestamp
        self.buf[12..20].copy_from_slice(&self.send_timestamp_ns.to_le_bytes());
        // Message Count
        self.buf[20] = self.msg_count;
        // Reserved
        self.buf[21] = 0;
        // Frame Length
        self.buf[22..24].copy_from_slice(&frame_length.to_le_bytes());

        &self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_header_layout() {
        let mut fb = FrameBuilder::new(3, 42, 1_700_000_000_000_000_000, DEFAULT_MTU);
        let buf = fb.message_buffer(8).unwrap();
        buf.copy_from_slice(&[0xAA; 8]);
        fb.commit_message();
        let frame = fb.finalize();

        assert_eq!(frame.len(), 32);
        assert_eq!(&frame[0..2], &[0x5A, 0x44]);
        assert_eq!(frame[2], 1);
        assert_eq!(frame[3], 3);
        assert_eq!(u64::from_le_bytes(frame[4..12].try_into().unwrap()), 42);
        assert_eq!(
            u64::from_le_bytes(frame[12..20].try_into().unwrap()),
            1_700_000_000_000_000_000
        );
        assert_eq!(frame[20], 1);
        assert_eq!(frame[21], 0);
        assert_eq!(u16::from_le_bytes(frame[22..24].try_into().unwrap()), 32);
    }

    #[test]
    fn remaining_tracks_capacity() {
        let mut fb = FrameBuilder::new(0, 0, 0, 100);
        assert_eq!(fb.remaining(), 76);
        let buf = fb.message_buffer(60).unwrap();
        buf.copy_from_slice(&[0; 60]);
        fb.commit_message();
        assert_eq!(fb.remaining(), 16);
    }

    #[test]
    fn exceeds_mtu_returns_error() {
        let mut fb = FrameBuilder::new(0, 0, 0, 30);
        let result = fb.message_buffer(10);
        assert_eq!(
            result.unwrap_err(),
            FrameError::ExceedsMtu {
                msg_size: 10,
                remaining: 6,
            }
        );
    }

    #[test]
    fn multiple_messages_in_frame() {
        let mut fb = FrameBuilder::new(0, 0, 0, DEFAULT_MTU);
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
    fn is_empty_tracks_messages() {
        let mut fb = FrameBuilder::new(0, 0, 0, DEFAULT_MTU);
        assert!(fb.is_empty());
        let buf = fb.message_buffer(4).unwrap();
        buf.copy_from_slice(&[0; 4]);
        fb.commit_message();
        assert!(!fb.is_empty());
    }
}
