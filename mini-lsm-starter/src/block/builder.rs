#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

use super::Block;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let added_size = key.len() + value.len() + SIZEOF_U16 * 3;
        if self.data.len() + added_size > self.block_size && !self.is_empty() {
            return false;
        }

        // put offset
        self.offsets.push(self.data.len() as u16);

        // put key
        self.data.put_u16(key.len() as u16);
        self.data.put(&key.raw_ref()[..]);
        // put value
        self.data.put_u16(value.len() as u16);
        self.data.put(&value[..]);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec()
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("can not finalize for empty block")
        }
        Block {
            offsets: self.offsets,
            data: self.data,
        }
    }
}
