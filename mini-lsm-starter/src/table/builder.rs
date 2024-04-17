#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = Vec::from(key.raw_ref())
        }

        if self.builder.add(key, value) {
            self.last_key = Vec::from(key.raw_ref());
            return;
        }

        self.finish_block();

        assert!(self.builder.add(key, value));
        self.first_key = Vec::from(key.raw_ref());
        self.last_key = Vec::from(key.raw_ref());
    }

    fn finish_block(&mut self) {
        // can only write like this
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(std::mem::take(&mut self.first_key))),
            last_key: KeyBytes::from_bytes(Bytes::from(std::mem::take(&mut self.last_key))),
        });

        let encoded_data = builder.build().encode();
        let check_sum = crc32fast::hash(encoded_data.as_ref());
        self.data.extend(encoded_data);
        self.data.put_u32(check_sum);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let first_key = self.first_key.clone();
        let last_key = self.last_key.clone();
        self.finish_block();
        let mut buf = self.data;
        let meta_offset = buf.len() as u32;
        // encode meta section
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        // encode meta length
        buf.put_u32(meta_offset);

        let file = FileObject::create(path.as_ref(), buf)?;

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::from(first_key)),
            last_key: KeyBytes::from_bytes(Bytes::from(last_key)),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
