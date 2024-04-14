#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{bail, Result};

use super::SsTable;
use crate::block::Block;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_inner(&table, 0)?;
        Ok(Self {
            table,
            blk_idx,
            blk_iter,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_inner(&self.table, 0)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    pub fn seek_to_next(&mut self) -> Result<()> {
        let (_, blk_iter) = Self::seek_to_inner(&self.table, self.blk_idx + 1)?;
        self.blk_idx += 1;
        self.blk_iter = blk_iter;
        Ok(())
    }

    fn seek_to_inner(table: &Arc<SsTable>, idx: usize) -> Result<(usize, BlockIterator)> {
        let blk = table.read_block(idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(blk);
        Ok((idx, blk_iter))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&table, key)?;
        Ok(Self {
            table,
            blk_idx,
            blk_iter,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        // NOTE: find_block_idx may not find the right blk_index when key is in the last blk
        let mut blk_idx = table.as_ref().find_block_idx(key);
        let mut blk_iterator =
            BlockIterator::create_and_seek_to_key(table.read_block(blk_idx)?, key);
        if !blk_iterator.is_valid() && blk_idx + 1 < table.num_of_blocks() {
            blk_idx += 1;
            (_, blk_iterator) = Self::seek_to_inner(table, blk_idx)?;
        }
        Ok((blk_idx, blk_iterator))
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.is_valid() && self.blk_idx + 1 < self.table.num_of_blocks() {
            self.seek_to_next()?;
        }

        Ok(())
    }
}
