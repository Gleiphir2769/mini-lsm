#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use crate::block::builder::SIZEOF_U16;
pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num_of_elements = self.offsets.len() as u16;
        let mut buf = self.data.clone();
        // put offset to buf
        self.offsets.iter().for_each(|offset| buf.put_u16(*offset));
        // put num_of_elements to the end
        buf.put_u16(num_of_elements);

        return Bytes::from(buf);
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 * (num_of_elements + 1);
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        let payload = &data[..data_end];

        let offsets: Vec<u16> = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut e| e.get_u16())
            .collect();

        Self {
            data: payload.to_vec(),
            offsets,
        }
    }
}
