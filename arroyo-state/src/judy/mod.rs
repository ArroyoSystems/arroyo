use std::io::SeekFrom;
use std::marker::PhantomData;
use std::time::SystemTime;
use std::{collections::BTreeMap, io::Cursor};

use std::convert::TryFrom;

use anyhow::{bail, Result};
use arroyo_types::{Data, Key};
use bincode::config::{self};
use bincode::{Decode, Encode};
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt};
use tracing::info;

pub mod backend;
pub mod tables;

pub struct JudyNode {
    children: BTreeMap<u8, JudyNode>,
    offset_contribution: Option<u32>,
    // these are bytes that are common to the terminal node and all children.
    // when traversing, append these to the key you're building up.
    common_bytes: Vec<u8>,
    terminal_offset: Option<u32>,
    is_root: bool,
}

pub struct JudyWriter {
    judy_node: JudyNode,
    bytes_written: usize,
    data_buffer: Vec<u8>,
}

impl JudyWriter {
    pub fn new() -> Self {
        JudyWriter {
            judy_node: JudyNode::new(),
            bytes_written: 0,
            data_buffer: Vec::new(),
        }
    }

    pub async fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let offset = self.bytes_written.try_into()?;
        self.bytes_written += 4 + value.len();
        // TODO: not use 4 full bytes every time.
        self.data_buffer
            .write_u32_le(value.len().try_into()?)
            .await?;
        self.data_buffer.write_all(value).await?;
        self.judy_node.insert(key, offset);
        Ok(())
    }

    pub async fn serialize<W: tokio::io::AsyncWrite + Unpin + std::marker::Send>(
        &mut self,
        writer: &mut W,
    ) -> Result<()> {
        self.judy_node.serialize(writer).await?;
        writer.write_all(&self.data_buffer).await?;
        writer.flush().await?;
        Ok(())
    }
}

impl JudyNode {
    pub fn new() -> Self {
        JudyNode {
            children: BTreeMap::new(),
            offset_contribution: None,
            common_bytes: Vec::new(),
            terminal_offset: None,
            is_root: true,
        }
    }
    pub fn insert(&mut self, key: &[u8], offset: u32) {
        if key.is_empty() {
            self.terminal_offset = Some(offset);
            return;
        }

        self.children
            .entry(key[0])
            .or_insert_with(|| JudyNode {
                children: BTreeMap::new(),
                offset_contribution: None,
                common_bytes: Vec::new(),
                terminal_offset: None,
                is_root: false,
            })
            .insert(&key[1..], offset);
    }

    // return the earliest data in the node and subnodes.
    // this should be an absolute value, and the min of terminal_offset.
    fn earliest_data(&self) -> u32 {
        match self.terminal_offset {
            Some(offset) => offset,
            None => {
                let Some(first_child) = self.children.values().next() else {
                    unreachable!("non-terminal JudyNode with no children");
                };
                first_child.earliest_data()
            }
        }
    }

    fn optimize_offsets(&mut self, parent_offset: u32) {
        let node_data_start = self.earliest_data();
        self.offset_contribution = Some(node_data_start - parent_offset);
        for child in self.children.values_mut() {
            child.optimize_offsets(node_data_start);
        }
    }

    fn compress_nodes(&mut self) {
        for child in self.children.values_mut() {
            child.compress_nodes();
        }
        if self.children.len() == 1 && self.terminal_offset.is_none() && !self.is_root {
            // this is a node that doesn't have data and one child, so merge it with its child.
            let (trie_byte, child_node) = self.children.pop_first().unwrap();
            self.common_bytes.push(trie_byte);
            self.common_bytes.extend(child_node.common_bytes);
            self.children = child_node.children;
            self.terminal_offset = child_node.terminal_offset;
        } else if self.terminal_offset.is_none() && !self.children.is_empty() && !self.is_root {
            // this node doesn't have data, so we can move the offset contribution up to the parent.
            if let Some(min_child_offset) = self
                .children
                .values()
                .map(|child| child.offset_contribution.unwrap_or_default())
                .min()
            {
                self.offset_contribution =
                    Some(min_child_offset + self.offset_contribution.unwrap_or_default());
                self.children.values_mut().for_each(|child| {
                    let new_offset =
                        child.offset_contribution.unwrap_or_default() - min_child_offset;
                    if new_offset > 0 {
                        child.offset_contribution = Some(new_offset);
                    } else {
                        child.offset_contribution = None;
                    }
                })
            }
        }
    }
    pub async fn serialize<W: tokio::io::AsyncWrite + Unpin + std::marker::Send>(
        &mut self,
        writer: &mut W,
    ) -> Result<()> {
        self.compress_nodes();
        self.optimize_offsets(0);

        self.serialize_internal(writer).await?;
        writer.flush().await?;
        Ok(())
    }

    #[async_recursion::async_recursion]
    async fn serialize_internal<W: tokio::io::AsyncWrite + Unpin + std::marker::Send>(
        &self,
        writer: &mut W,
    ) -> Result<()> {
        let serialization_stats = self.serialization_stats();
        let offset_contribution = if self.is_root {
            if self.offset_contribution.unwrap_or_default() > 0 {
                bail!(
                    "Root node should have 0 as offset contribution, not {}",
                    self.offset_contribution.unwrap_or_default()
                );
            }
            Some(serialization_stats.total_size as u32)
        } else {
            self.offset_contribution
        };
        let header = serialization_stats.header;

        writer.write_u8(header.to_byte()).await?;
        if !self.common_bytes.is_empty() {
            writer.write(&[self.common_bytes.len() as u8]).await?;
            writer.write_all(&self.common_bytes).await?;
        }
        header
            .offset_contribution_size
            .write(offset_contribution, writer)
            .await?;
        if self.children.is_empty() {
            return Ok(());
        }
        header
            .children_density
            .write_keys(serialization_stats.keys, writer)
            .await?;
        header
            .write_pointers(serialization_stats.map_offsets, writer)
            .await?;
        for child in self.children.values() {
            if !child.is_simple_leaf() {
                child.serialize_internal(writer).await?;
            }
        }
        Ok(())
    }

    fn serialization_stats(&self) -> SerializationStats {
        let mut size = 1; // 1 byte for the flag

        let has_common_bytes = if self.common_bytes.is_empty() {
            HasCommonBytes::No
        } else {
            HasCommonBytes::Yes
        };

        size += has_common_bytes.byte_count();
        size += self.common_bytes.len();

        // special case for root so we can set the initial location of the data.
        let offset_contribution_size = if self.is_root {
            VariableByteSize::FourBytes
        } else {
            match self.offset_contribution {
                Some(val) if val <= u8::MAX as u32 => VariableByteSize::OneByte,
                Some(val) if 2 * val + 1 <= u16::MAX as u32 => VariableByteSize::TwoBytes,
                Some(_) => VariableByteSize::FourBytes,
                None => VariableByteSize::None,
            }
        };

        size += offset_contribution_size.byte_count();
        let has_terminal_value = if self.terminal_offset.is_some() {
            HasTerminalValue::Yes
        } else {
            HasTerminalValue::No
        };

        // if you don't have children. This should only happen if you've compressed a leaf node,
        // in which case the separate node is needed to store the common_bytes.
        if self.children.is_empty() {
            return SerializationStats::new(
                size,
                JudyNodeHeader::new(
                    offset_contribution_size,
                    has_terminal_value,
                    ChildrenDensity::Sparse,
                    VariableByteSize::None,
                    has_common_bytes,
                ),
                vec![],
                vec![],
            );
        }
        let children_density = if self.children.len() <= 16 {
            size += 1 + self.children.len();
            ChildrenDensity::Sparse
        } else {
            size += 32;
            ChildrenDensity::Dense
        };

        let mut offsets = vec![];
        let mut children_size = 0;
        for (key, child) in &self.children {
            if child.is_simple_leaf() {
                offsets.push((
                    *key,
                    child.offset_contribution.unwrap_or_default() as usize,
                    true,
                ));
            } else {
                let child_size = child.serialization_stats().total_size;
                offsets.push((*key, children_size, false));
                children_size += child_size;
            }
        }
        let largest_offset = offsets
            .iter()
            .map(|(_, offset, _)| offset)
            .max()
            .unwrap()
            .clone();

        // need to figure out how many bytes we need to encode the largest offset.
        // the index offsets will be relative to the index we're currently creating, so need to take that into account.
        let mut child_pointer_size = VariableByteSize::FourBytes;
        size += child_pointer_size.byte_count() * offsets.len();
        if largest_offset + size < i16::MAX as usize {
            size -= child_pointer_size.byte_count() * offsets.len();
            child_pointer_size = VariableByteSize::TwoBytes;
            size += child_pointer_size.byte_count() * offsets.len();
            if largest_offset + size < i8::MAX as usize {
                size -= child_pointer_size.byte_count() * offsets.len();
                child_pointer_size = VariableByteSize::OneByte;
                size += child_pointer_size.byte_count() * offsets.len();
            }
        }
        // right now size is of the data for the specific node. We can now compute the values to actually write to the map.
        let (keys, map_values): (Vec<_>, Vec<_>) = offsets
            .iter()
            .map(|(key, offset, is_leaf)| {
                if *is_leaf {
                    let offset = (*offset as i32) * -1;
                    (*key, offset)
                } else {
                    (*key, (*offset as i32) + size as i32)
                }
            })
            .unzip();
        SerializationStats::new(
            size + children_size,
            JudyNodeHeader::new(
                offset_contribution_size,
                has_terminal_value,
                children_density,
                child_pointer_size,
                has_common_bytes,
            ),
            keys,
            map_values,
        )
    }

    fn is_simple_leaf(&self) -> bool {
        self.children.is_empty() && self.common_bytes.is_empty()
    }

    #[async_recursion::async_recursion]
    pub async fn get_data<R: tokio::io::AsyncRead + Unpin + Send + AsyncSeek>(
        index_reader: &mut R,
        data_reader: &mut BytesWithOffset,
        mut key: &[u8],
    ) -> Result<Option<Bytes>> {
        let starting_position = index_reader.stream_position().await?;

        let mut header_byte = [0u8; 1];
        index_reader.read_exact(&mut header_byte).await?;
        let header = JudyNodeHeader::try_from(header_byte[0])?;

        if header.has_common_bytes == HasCommonBytes::Yes {
            let mut common_len = [0u8; 1];
            index_reader.read_exact(&mut common_len).await?;
            let mut common_bytes = vec![0u8; common_len[0] as usize];
            index_reader.read_exact(&mut common_bytes).await?;
            if !key.starts_with(&common_bytes) {
                return Ok(None);
            }
            key = &key[common_bytes.len()..];
        }
        let offset_contribution = header
            .offset_contribution_size
            .read_u32(index_reader)
            .await?;
        if let Some(offset_contribution) = offset_contribution {
            data_reader.seek(SeekFrom::Current(offset_contribution as i64))?;
        }
        if key.is_empty() {
            if header.has_terminal_value() {
                let value_size = data_reader.read_u32()?;
                return Ok(Some(data_reader.read_next_n_bytes(value_size)?));
            } else {
                return Ok(None);
            }
        }
        let next_byte = key[0];
        key = &key[1..];
        let Some(child_offset) = header
            .get_child_offset_async(next_byte, index_reader)
            .await?
        else {
            return Ok(None);
        };
        if child_offset <= 0 {
            data_reader.seek(SeekFrom::Current(-child_offset as i64))?;
            let value_size = data_reader.read_u32()?;
            Ok(Some(data_reader.read_next_n_bytes(value_size)?))
        } else {
            index_reader
                .seek(SeekFrom::Start(starting_position + child_offset as u64))
                .await?;
            Self::get_data(index_reader, data_reader, key).await
        }
    }

    pub async fn get_btree_map_from_bytes(bytes: Bytes) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut index_reader = Cursor::new(bytes.clone());
        let mut data_reader = BytesWithOffset::new(bytes);
        let mut map = BTreeMap::new();
        Self::fill_btree_map(&mut index_reader, &mut data_reader, &mut map, vec![]).await?;
        Ok(map)
    }

    #[async_recursion::async_recursion]
    pub async fn fill_btree_map<R: tokio::io::AsyncRead + Unpin + AsyncSeek + Send>(
        index_reader: &mut R,
        data_reader: &mut BytesWithOffset,
        map: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        mut prefix: Vec<u8>,
    ) -> Result<()> {
        let starting_position = index_reader.stream_position().await?;
        let mut header_byte = [0u8; 1];
        index_reader.read_exact(&mut header_byte).await?;
        let header = JudyNodeHeader::try_from(header_byte[0])?;
        if header.has_common_bytes == HasCommonBytes::Yes {
            let mut common_len = [0u8; 1];
            index_reader.read_exact(&mut common_len).await?;
            let mut common_bytes = vec![0u8; common_len[0] as usize];
            index_reader.read_exact(&mut common_bytes).await?;
            prefix.extend(common_bytes);
        }
        // Advance offset
        if let Some(offset_contribution) = header
            .offset_contribution_size
            .read_u32(index_reader)
            .await?
        {
            data_reader.seek(SeekFrom::Current(offset_contribution as i64))?;
        }
        let starting_data_position = data_reader.offset();

        if header.has_terminal_value() {
            let value_size = data_reader.read_u32()?;

            map.insert(
                prefix.clone(),
                data_reader.read_next_n_bytes(value_size)?.to_vec(),
            );
        }
        if header.child_pointer_size == VariableByteSize::None {
            return Ok(());
        }
        let (keys, offsets) = match header.children_density {
            ChildrenDensity::Sparse => {
                let key_count = (index_reader.read_u8().await? as usize) + 1;
                let mut keys = vec![0; key_count];
                let mut offsets = Vec::with_capacity(key_count);
                index_reader.read_exact(&mut keys).await?;
                for i in 0..key_count {
                    let Some(offset) = header.child_pointer_size.read_i32(index_reader).await?
                    else {
                        bail!("Unexpected null offset");
                    };
                    offsets.push(offset);
                }
                (keys, offsets)
            }
            ChildrenDensity::Dense => {
                // load 32 byte bitmap
                let mut bitmap = [0u8; 32];
                index_reader.read_exact(&mut bitmap).await?;
                // find the 1s in the bitmap, and read the offset
                let mut keys = vec![];
                let mut offsets = vec![];
                for i in 0..32 {
                    for j in 0..8 {
                        if bitmap[i] & (1 << j) != 0 {
                            keys.push((i * 8 + j) as u8);
                            offsets.push(
                                header
                                    .child_pointer_size
                                    .read_i32(index_reader)
                                    .await?
                                    .unwrap(),
                            );
                        }
                    }
                }
                (keys, offsets)
            }
        };
        let key_offset_map: BTreeMap<_, _> = keys.iter().zip(offsets).collect();
        for (key, offset) in key_offset_map {
            let mut new_prefix = prefix.clone();
            new_prefix.push(*key);
            if offset <= 0 {
                data_reader.seek(SeekFrom::Start(
                    (starting_data_position as i32 - offset) as u64,
                ))?;
                let value_size = data_reader.read_u32()?;
                map.insert(
                    new_prefix,
                    data_reader.read_next_n_bytes(value_size)?.to_vec(),
                );
            } else {
                data_reader.seek(SeekFrom::Start(starting_data_position))?;
                let x = index_reader
                    .seek(SeekFrom::Start(starting_position + offset as u64))
                    .await?;
                Self::fill_btree_map(index_reader, data_reader, map, new_prefix).await?;
            }
        }
        Ok(())
    }

    async fn get_keys<K: Decode + Send>(bytes: Bytes) -> Result<Vec<K>> {
        let mut index_reader = Cursor::new(bytes.clone());
        Self::get_keys_internal(&mut index_reader, vec![]).await
    }

    #[async_recursion::async_recursion]
    async fn get_keys_internal<
        K: Decode + Send,
        R: tokio::io::AsyncRead + Unpin + AsyncSeek + Send,
    >(
        index_reader: &mut R,
        mut prefix: Vec<u8>,
    ) -> Result<Vec<K>> {
        let starting_position = index_reader.stream_position().await?;
        let mut header_byte = [0u8; 1];
        index_reader.read_exact(&mut header_byte).await?;
        let header = JudyNodeHeader::try_from(header_byte[0])?;
        if header.has_common_bytes == HasCommonBytes::Yes {
            let mut common_len = [0u8; 1];
            index_reader.read_exact(&mut common_len).await?;
            let mut common_bytes = vec![0u8; common_len[0] as usize];
            index_reader.read_exact(&mut common_bytes).await?;
            prefix.extend(common_bytes);
        }
        index_reader
            .seek(SeekFrom::Current(
                header.offset_contribution_size.byte_count() as i64,
            ))
            .await?;
        let (keys, offsets) = match header.children_density {
            ChildrenDensity::Sparse => {
                let key_count = (index_reader.read_u8().await? as usize) + 1;
                let mut keys = vec![0; key_count];
                let mut offsets = Vec::with_capacity(key_count);
                index_reader.read_exact(&mut keys).await?;
                for i in 0..key_count {
                    let Some(offset) = header.child_pointer_size.read_i32(index_reader).await?
                    else {
                        bail!("Unexpected null offset");
                    };
                    offsets.push(offset);
                }
                (keys, offsets)
            }
            ChildrenDensity::Dense => {
                // load 32 byte bitmap
                let mut bitmap = [0u8; 32];
                index_reader.read_exact(&mut bitmap).await?;
                // find the 1s in the bitmap, and read the offset
                let mut keys = vec![];
                let mut offsets = vec![];
                for i in 0..32 {
                    for j in 0..8 {
                        if bitmap[i] & (1 << j) != 0 {
                            keys.push((i * 8 + j) as u8);
                            offsets.push(
                                header
                                    .child_pointer_size
                                    .read_i32(index_reader)
                                    .await?
                                    .unwrap(),
                            );
                        }
                    }
                }
                (keys, offsets)
            }
        };
        let mut decoded_keys = vec![];
        if header.has_terminal_value() {
            decoded_keys.push(bincode::decode_from_slice(&prefix, config::standard())?.0);
        }
        for (key, offset) in keys.into_iter().zip(offsets.into_iter()) {
            prefix.push(key);
            if offset <= 0 {
                decoded_keys.push(bincode::decode_from_slice(&prefix, config::standard())?.0);
            } else {
                index_reader
                    .seek(SeekFrom::Start(starting_position + offset as u64))
                    .await?;
                decoded_keys.extend(Self::get_keys_internal(index_reader, prefix.clone()).await?);
            }
            prefix.pop();
        }
        Ok(decoded_keys)
    }
}
pub struct BytesWithOffset {
    bytes: Bytes,
    position: u64,
}

impl BytesWithOffset {
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes, position: 0 }
    }

    fn offset(&self) -> u64 {
        self.position
    }

    fn new_from_vec(bytes: Vec<u8>) -> Self {
        BytesWithOffset {
            bytes: Bytes::from(bytes),
            position: 0,
        }
    }
    fn seek(&mut self, seek_from: SeekFrom) -> Result<u64> {
        match seek_from {
            SeekFrom::Start(start) => self.position = start,
            SeekFrom::End(end) => {
                if (self.bytes.len() as i64) + end < 0 {
                    bail!("Cannot seek to before the byte array starts");
                }
                self.position = ((self.bytes.len() as i64) + end) as u64;
            }
            SeekFrom::Current(current) => {
                if (self.position as i64) + current < 0 {
                    bail!("Cannot seek to before the byte array starts");
                }
                self.position = ((self.position as i64) + current) as u64;
            }
        }
        Ok(self.position)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes_to_read = self
            .bytes
            .slice(self.position as usize..self.position as usize + 4);
        self.position += 4;
        Ok(LittleEndian::read_u32(&bytes_to_read))
    }

    fn read_next_n_bytes(&mut self, n: u32) -> Result<Bytes> {
        let bytes_to_read = self
            .bytes
            .slice(self.position as usize..self.position as usize + n as usize);
        self.position += n as u64;
        Ok(bytes_to_read)
    }
}

struct SerializationStats {
    total_size: usize,
    header: JudyNodeHeader,
    keys: Vec<u8>,
    map_offsets: Vec<i32>,
}

impl SerializationStats {
    fn new(
        total_size: usize,
        header: JudyNodeHeader,
        keys: Vec<u8>,
        map_offsets: Vec<i32>,
    ) -> Self {
        SerializationStats {
            total_size,
            header,
            keys,
            map_offsets,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum VariableByteSize {
    None = 0b00,
    OneByte = 0b01,
    TwoBytes = 0b10,
    FourBytes = 0b11,
}
impl VariableByteSize {
    fn byte_count(&self) -> usize {
        match self {
            VariableByteSize::None => 0,
            VariableByteSize::OneByte => 1,
            VariableByteSize::TwoBytes => 2,
            VariableByteSize::FourBytes => 4,
        }
    }

    async fn write<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        offset_contribution: Option<u32>,
        writer: &mut W,
    ) -> Result<()> {
        match self {
            VariableByteSize::None => {}
            VariableByteSize::OneByte => {
                writer.write_u8(offset_contribution.unwrap() as u8).await?;
            }
            VariableByteSize::TwoBytes => {
                writer
                    .write_u16_le(offset_contribution.unwrap() as u16)
                    .await?;
            }
            VariableByteSize::FourBytes => {
                writer.write_u32_le(offset_contribution.unwrap()).await?;
            }
        }
        Ok(())
    }

    fn read_u32_sync(&self, reader: &mut Cursor<Bytes>) -> Result<Option<u32>> {
        match self {
            VariableByteSize::None => Ok(None),
            VariableByteSize::OneByte => Ok(Some(byteorder::ReadBytesExt::read_u8(reader)? as u32)),
            VariableByteSize::TwoBytes => Ok(Some(
                byteorder::ReadBytesExt::read_u16::<LittleEndian>(reader)? as u32,
            )),
            VariableByteSize::FourBytes => Ok(Some(byteorder::ReadBytesExt::read_u32::<
                LittleEndian,
            >(reader)?)),
        }
    }

    fn read_i32_sync(&self, reader: &mut Cursor<Bytes>) -> Result<Option<i32>> {
        match self {
            VariableByteSize::None => Ok(None),
            VariableByteSize::OneByte => Ok(Some(byteorder::ReadBytesExt::read_i8(reader)? as i32)),
            VariableByteSize::TwoBytes => Ok(Some(
                byteorder::ReadBytesExt::read_i16::<LittleEndian>(reader)? as i32,
            )),
            VariableByteSize::FourBytes => Ok(Some(byteorder::ReadBytesExt::read_i32::<
                LittleEndian,
            >(reader)?)),
        }
    }

    async fn read_u32<R: tokio::io::AsyncRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> Result<Option<u32>> {
        match self {
            VariableByteSize::None => Ok(None),
            VariableByteSize::OneByte => {
                let mut buf = [0u8; 1];
                reader.read_exact(&mut buf).await?;
                Ok(Some(buf[0] as u32))
            }
            VariableByteSize::TwoBytes => {
                let mut buf = [0u8; 2];
                reader.read_exact(&mut buf).await?;
                Ok(Some(u16::from_le_bytes(buf) as u32))
            }
            VariableByteSize::FourBytes => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf).await?;
                Ok(Some(u32::from_le_bytes(buf)))
            }
        }
    }
    async fn read_i32<R: tokio::io::AsyncRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> Result<Option<i32>> {
        match self {
            VariableByteSize::None => Ok(None),
            VariableByteSize::OneByte => {
                let mut buf = [0u8; 1];
                reader.read_exact(&mut buf).await?;
                Ok(Some((buf[0] as i8) as i32))
            }
            VariableByteSize::TwoBytes => {
                let mut buf = [0u8; 2];
                reader.read_exact(&mut buf).await?;
                Ok(Some(i16::from_le_bytes(buf) as i32))
            }
            VariableByteSize::FourBytes => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf).await?;
                Ok(Some(i32::from_le_bytes(buf)))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum HasTerminalValue {
    No = 0,
    Yes = 1,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ChildrenDensity {
    Sparse = 0,
    Dense = 1,
}
impl ChildrenDensity {
    async fn write_keys<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        keys: Vec<u8>,
        writer: &mut W,
    ) -> Result<()> {
        match self {
            ChildrenDensity::Sparse => {
                writer.write_u8((keys.len() - 1) as u8).await?;
                for key in keys {
                    writer.write_u8(key).await?;
                }
            }
            ChildrenDensity::Dense => {
                let mut bitmap = vec![0; 32];
                for key in keys {
                    let byte_index = key / 8;
                    let bit_index = key % 8;
                    bitmap[byte_index as usize] |= 1 << bit_index;
                }
                writer.write_all(&bitmap).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum HasCommonBytes {
    No = 0,
    Yes = 1,
}
impl HasCommonBytes {
    fn byte_count(&self) -> usize {
        match self {
            HasCommonBytes::No => 0,
            HasCommonBytes::Yes => 1,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct JudyNodeHeader {
    offset_contribution_size: VariableByteSize,
    has_terminal_value: HasTerminalValue,
    children_density: ChildrenDensity,
    child_pointer_size: VariableByteSize,
    has_common_bytes: HasCommonBytes,
}

impl JudyNodeHeader {
    fn new(
        offset_contribution_size: VariableByteSize,
        has_terminal_value: HasTerminalValue,
        children_density: ChildrenDensity,
        child_pointer_size: VariableByteSize,
        has_common_bytes: HasCommonBytes,
    ) -> Self {
        JudyNodeHeader {
            offset_contribution_size,
            has_terminal_value,
            children_density,
            child_pointer_size,
            has_common_bytes,
        }
    }

    // Convert the struct into a single byte flag
    fn to_byte(&self) -> u8 {
        let mut flag = 0u8;

        flag |= (self.offset_contribution_size as u8) & 0b11; // Bits 0-1
        flag |= (self.has_terminal_value as u8) << 2; // Bit 2
        flag |= (self.children_density as u8) << 3; // Bit 3
        flag |= (self.child_pointer_size as u8) << 4; // Bits 4-5
        flag |= (self.has_common_bytes as u8) << 6; // Bit 6

        flag
    }

    fn has_terminal_value(&self) -> bool {
        self.has_terminal_value == HasTerminalValue::Yes
    }

    async fn get_child_offset_async<R: tokio::io::AsyncRead + Unpin + AsyncSeek>(
        &self,
        byte: u8,
        reader: &mut R,
    ) -> Result<Option<i32>> {
        match self.children_density {
            ChildrenDensity::Sparse => {
                let key_count = (reader.read_u8().await? as usize) + 1;
                // scan key_count, looking for bytes
                for i in 0..key_count {
                    let key = reader.read_u8().await?;
                    if key == byte {
                        reader
                            .seek(SeekFrom::Current(
                                ((key_count - i - 1) + self.child_pointer_size.byte_count() * i)
                                    as i64,
                            ))
                            .await?;
                        return Ok(self.child_pointer_size.read_i32(reader).await?);
                    } else if byte < key {
                        return Ok(None);
                    }
                }
                Ok(None)
            }
            ChildrenDensity::Dense => {
                // advance key/8
                let mut bitmap = [0u8; 32];
                reader.read_exact(&mut bitmap).await?;
                // if the bit is set, it will be the Nth offset
                let byte_index = (byte / 8) as usize;
                let bit_index = byte % 8;
                if bitmap[byte_index] & (1 << bit_index) == 0 {
                    return Ok(None);
                }
                let mut rank = 0;
                for i in 0..byte_index {
                    rank += bitmap[i as usize].count_ones() as usize;
                }
                rank += (bitmap[byte_index] & ((1 << bit_index) - 1)).count_ones() as usize;
                reader
                    .seek(SeekFrom::Current(
                        (rank * self.child_pointer_size.byte_count()) as i64,
                    ))
                    .await?;
                Ok(self.child_pointer_size.read_i32(reader).await?)
            }
        }
    }

    async fn write_pointers<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        map_offsets: Vec<i32>,
        writer: &mut W,
    ) -> Result<()> {
        match self.child_pointer_size {
            VariableByteSize::OneByte => {
                for offset in map_offsets {
                    writer.write_i8(offset as i8).await?;
                }
            }
            VariableByteSize::TwoBytes => {
                for offset in map_offsets {
                    writer.write_i16_le(offset as i16).await?;
                }
            }
            VariableByteSize::FourBytes => {
                for offset in map_offsets {
                    writer.write_i32_le(offset as i32).await?;
                }
            }
            VariableByteSize::None => {
                bail!("shouldn't be writing pointers without a valid byte size")
            }
        }
        Ok(())
    }
}

impl TryFrom<u8> for JudyNodeHeader {
    type Error = anyhow::Error; // You can use a custom Error type here

    fn try_from(value: u8) -> Result<Self> {
        let offset_contribution_size = match value & 0b11 {
            0 => VariableByteSize::None,
            1 => VariableByteSize::OneByte,
            2 => VariableByteSize::TwoBytes,
            3 => VariableByteSize::FourBytes,
            _ => bail!("Invalid offset contribution size".to_string()),
        };

        let has_terminal_value = match (value >> 2) & 1 {
            0 => HasTerminalValue::No,
            1 => HasTerminalValue::Yes,
            _ => bail!("Invalid terminal value flag".to_string()),
        };

        let children_density = match (value >> 3) & 1 {
            0 => ChildrenDensity::Sparse,
            1 => ChildrenDensity::Dense,
            _ => bail!("Invalid children density flag".to_string()),
        };

        let child_pointer_size = match (value >> 4) & 0b11 {
            0 => VariableByteSize::None,
            1 => VariableByteSize::OneByte,
            2 => VariableByteSize::TwoBytes,
            3 => VariableByteSize::FourBytes,
            _ => bail!("Invalid child pointer size".to_string()),
        };

        let has_common_bytes = match (value >> 6) & 1 {
            0 => HasCommonBytes::No,
            1 => HasCommonBytes::Yes,
            _ => bail!("Invalid common bytes flag".to_string()),
        };

        Ok(JudyNodeHeader {
            offset_contribution_size,
            has_terminal_value,
            children_density,
            child_pointer_size,
            has_common_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        io::{Cursor, SeekFrom},
        time::SystemTime,
    };

    use crate::judy::{
        tables::reader::JudyReader, BytesWithOffset, ImpulseSourceState,
        PeriodicWatermarkGeneratorState,
    };

    use super::{JudyNode, JudyWriter};
    use bincode::config;
    use bytes::Bytes;
    use rand::{rngs::SmallRng, Rng, SeedableRng};
    // Adjust the import as necessary
    use anyhow::Result;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    struct TestHarness {
        writer: JudyWriter,
        serialized_data: Vec<u8>,
        finished: Option<Bytes>,
    }

    impl TestHarness {
        pub fn new() -> Self {
            Self {
                writer: JudyWriter::new(),
                serialized_data: Vec::new(),
                finished: None,
            }
        }

        pub async fn add_data(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
            self.writer.insert(key, data).await?;
            Ok(())
        }

        pub async fn serialize_files(&mut self) -> Result<()> {
            self.writer.serialize(&mut self.serialized_data).await?;
            self.finished = Some(Bytes::from(self.serialized_data.clone()));
            Ok(())
        }

        pub async fn test_retrieval(
            &self,
            key: &[u8],
            expected_data: Option<Vec<u8>>,
        ) -> Result<()> {
            let mut cursor = Cursor::new(self.finished.clone().unwrap());
            cursor.seek(SeekFrom::Start(0)).await?;
            let result = JudyNode::get_data(
                &mut cursor,
                &mut BytesWithOffset::new(self.finished.clone().unwrap()),
                key,
            )
            .await?;
            assert_eq!(result.map(|result| result.to_vec()), expected_data);
            Ok(())
        }
        pub async fn get_btree_map(&self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
            let mut result = BTreeMap::new();
            let mut index_reader = Cursor::new(self.serialized_data.clone());
            let prefix = vec![];
            JudyNode::fill_btree_map(
                &mut index_reader,
                &mut BytesWithOffset::new(self.finished.clone().unwrap()),
                &mut result,
                prefix,
            )
            .await?;
            Ok(result)
        }
    }

    #[tokio::test]
    async fn judy_node_test() -> Result<()> {
        let mut harness = TestHarness::new();

        // Add data to the test
        harness
            .add_data(
                vec![0x6b, 0x65, 0x79, 0x31].as_slice(),
                vec![1, 2, 3, 4].as_slice(),
            )
            .await?;
        harness
            .add_data(
                vec![0x6b, 0x65, 0x79, 0x32].as_slice(),
                vec![3, 4, 5, 6, 7].as_slice(),
            )
            .await?;

        // Serialize files
        harness.serialize_files().await?;

        // Test retrieval
        harness
            .test_retrieval(&vec![0x6b, 0x65, 0x79, 0x31], Some(vec![1, 2, 3, 4]))
            .await?;
        harness
            .test_retrieval(&vec![0x6b, 0x65, 0x79, 0x32], Some(vec![3, 4, 5, 6, 7]))
            .await?;
        harness
            .test_retrieval(&vec![0x6b, 0x65, 0x79, 0x33], None)
            .await?;

        Ok(())
    }
    async fn run_random_test(
        depth: usize,
        density: f32,
        data_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut harness = TestHarness::new(); // Add constructor or initialization logic
        let mut expected_map = BTreeMap::new();
        let mut rng = SmallRng::seed_from_u64(0);

        // Use depth, density, and data_size to generate your JudyNode
        // Example: Varying keys based on 'depth' and 'density'
        let total_keys = ((1 << depth) as f32 * density).ceil() as usize;

        for _ in 0..total_keys {
            let key_len: usize = rng.gen_range(1..depth);
            let key: Vec<u8> = (0..key_len).map(|_| rng.gen()).collect();

            let data: Vec<u8> = (0..data_size).map(|_| rng.gen()).collect();

            expected_map.insert(key, data);
        }
        let mut i = 0;
        for (key, value) in &expected_map {
            i += 1;
            harness.add_data(key, value).await?;
        }

        harness.serialize_files().await?;

        i = 0;
        let mut reader: JudyReader<Vec<u8>, Vec<u8>> =
            JudyReader::new(harness.finished.as_ref().unwrap().clone())?;
        // Validate the JudyNode
        for (key, expected_data) in &expected_map {
            harness
                .test_retrieval(key, Some(expected_data.clone()))
                .await
                .expect(&format!("failed on key {}", i));
            let reader_bytes = reader.get_bytes(key)?;
            assert_eq!(reader_bytes, Some(expected_data.clone().into()));
            i += 1;
        }
        let generated_btree = harness.get_btree_map().await?;
        assert_eq!(generated_btree, expected_map);

        Ok(())
    }
    #[tokio::test]
    async fn variable_judy_node_test() -> Result<()> {
        run_random_test(5, 0.5, 32).await.unwrap();
        run_random_test(10, 0.7, 64).await.unwrap();
        run_random_test(7, 0.9, 16).await.unwrap();
        run_random_test(24, 0.001, 32).await.unwrap();
        Ok(())
    }

    #[derive(Clone, Debug, bincode::Encode, bincode::Decode, PartialEq, PartialOrd)]
    pub struct generated_struct_10845963830645997386 {
        pub bids_auction: Option<i64>,
    }

    #[tokio::test]
    async fn read_judy_array() -> Result<()> {
        let keys: Vec<Vec<u8>> = vec![];
        let judy_path = "/tmp/arroyo/job_TXDU51LDFM/checkpoints/checkpoint-0000003/operator-slow_impulse_0/table-i-000-0";
        let judy_file = tokio::fs::File::open(judy_path).await?;
        let mut memory_buffer = vec![];
        tokio::io::BufReader::new(judy_file)
            .read_to_end(&mut memory_buffer)
            .await?;
        let bytes: Bytes = memory_buffer.into();
        let mut file_index_reader =
            tokio::io::BufReader::new(tokio::fs::File::open(judy_path).await?);
        let mut file_data_reader =
            tokio::io::BufReader::new(tokio::fs::File::open(judy_path).await?);
        let mut memory_index_reader = Cursor::new(bytes.clone());
        let mut memory_data_reader = BytesWithOffset::new(bytes.clone());
        let mut in_memory = BTreeMap::new();
        JudyNode::fill_btree_map(
            &mut memory_index_reader,
            &mut memory_data_reader,
            &mut in_memory,
            vec![],
        )
        .await?;
        let hashmap: HashMap<Vec<u8>, Vec<u8>> = in_memory
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        println!("value at 0 is {:?}", in_memory.get(&vec![0]));

        let mut reader: JudyReader<usize, ImpulseSourceState> = JudyReader::new(bytes.clone())?;

        println!("value is {:?}", reader.get(&0));
        println!("vec is {:?}", reader.as_vec());

        for storage_type in vec![
            StorageType::Memory,
            StorageType::BTreeMap,
            StorageType::HashMap,
        ] {
            let now = SystemTime::now();
            let mut fetches = 0;
            for i in 0..200 {
                for key in keys.iter() {
                    fetches += 1;
                    match storage_type {
                        StorageType::Memory => {
                            memory_index_reader.seek(SeekFrom::Start(0)).await?;
                            memory_data_reader.seek(SeekFrom::Start(0))?;
                            JudyNode::get_data(
                                &mut memory_index_reader,
                                &mut memory_data_reader,
                                key,
                            )
                            .await?
                            .expect(&format!("should've found data for key {:?}", key));
                        }
                        StorageType::File => {
                            /*   file_index_reader.seek(SeekFrom::Start(0)).await?;
                            file_data_reader.seek(SeekFrom::Start(0)).await?;
                            JudyNode::get_data(&mut file_index_reader, &mut file_data_reader, key)
                                .await?
                                .expect(&format!("should've found data for key {:?}", key));*/
                        }
                        StorageType::ReOpenFile => {
                            /* let mut file_index_reader =
                                tokio::io::BufReader::new(tokio::fs::File::open(judy_path).await?);
                            let mut file_data_reader =
                                tokio::io::BufReader::new(tokio::fs::File::open(judy_path).await?);
                            JudyNode::get_data(&mut file_index_reader, &mut file_data_reader, key)
                                .await?
                                .expect(&format!("should've found data for key {:?}", key)); */
                        }
                        StorageType::BTreeMap => {
                            in_memory
                                .get(key)
                                .expect(&format!("should've found data for key {:?}", key));
                        }
                        StorageType::HashMap => {
                            hashmap
                                .get(key)
                                .expect(&format!("should've found data for key {:?}", key));
                        }
                    }
                }
            }
            println!(
                "fetching with {} times with {:?} took {:?}",
                fetches,
                storage_type,
                now.elapsed()?
            );
        }
        Ok(())
    }
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum StorageType {
        Memory,
        File,
        ReOpenFile,
        BTreeMap,
        HashMap,
    }
}

#[derive(Encode, Decode, Copy, Clone, Debug, PartialEq)]
pub struct PeriodicWatermarkGeneratorState {
    last_watermark_emitted_at: SystemTime,
    max_watermark: SystemTime,
}

#[derive(Encode, Decode, Debug, Copy, Clone, Eq, PartialEq)]
pub struct ImpulseSourceState {
    counter: usize,
    start_time: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InMemoryJudyNode<K, V> {
    children: BTreeMap<u8, InMemoryJudyNode<K, V>>,
    terminal_value: Option<V>,
    _phantom: PhantomData<K>,
}

impl<K, V> Default for InMemoryJudyNode<K, V> {
    fn default() -> Self {
        Self {
            children: BTreeMap::new(),
            terminal_value: None,
            _phantom: PhantomData,
        }
    }
}
unsafe impl<K, V> Sync for InMemoryJudyNode<K, V> {}

impl<K: Key, V> InMemoryJudyNode<K, V> {
    fn insert_bytes(&mut self, key: &[u8], value: V) {
        if key.is_empty() {
            self.terminal_value = Some(value);
            return;
        }
        let first_byte = key[0];
        let child = self.children.entry(first_byte).or_default();
        child.insert_bytes(&key[1..], value);
    }

    pub fn insert(&mut self, key: &K, value: V) -> Result<()> {
        let key_bytes = bincode::encode_to_vec(key, config::standard())?;
        self.insert_bytes(&key_bytes, value);
        Ok(())
    }

    fn get_bytes(&self, key: &[u8]) -> Option<&V> {
        if key.is_empty() {
            return self.terminal_value.as_ref();
        }
        let first_byte = key[0];
        self.children
            .get(&first_byte)
            .and_then(|child| child.get_bytes(&key[1..]))
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let key_bytes = bincode::encode_to_vec(key, config::standard()).unwrap();
        self.get_bytes(&key_bytes)
    }

    pub fn as_vec(&self) -> Vec<(K, &V)> {
        self.as_vec_internal(&[])
    }

    pub fn as_vec_internal(&self, prefix: &[u8]) -> Vec<(K, &V)> {
        let mut result = vec![];
        if let Some(terminal_value) = &self.terminal_value {
            result.push((
                bincode::decode_from_slice(prefix, config::standard())
                    .unwrap()
                    .0,
                terminal_value,
            ));
        }
        for (key, child) in self.children.iter() {
            let mut prefix = prefix.to_vec();
            prefix.push(*key);
            result.extend(child.as_vec_internal(&prefix));
        }
        result
    }

    pub fn get_mut_or_insert(&mut self, key: &K, insert_func: fn() -> V) -> &mut V {
        let key_bytes = bincode::encode_to_vec(key, config::standard()).unwrap();
        self.get_mut_or_insert_bytes(&key_bytes, insert_func)
    }
    fn get_mut_or_insert_bytes(&mut self, key: &[u8], insert_func: fn() -> V) -> &mut V {
        if key.is_empty() {
            if self.terminal_value.is_none() {
                self.terminal_value = Some(insert_func());
            }
            return self.terminal_value.as_mut().unwrap();
        }
        let first_byte = key[0];
        let child = self.children.entry(first_byte).or_default();
        child.get_mut_or_insert_bytes(&key[1..], insert_func)
    }
}

impl<K1: Key, K2: Key, V> InMemoryJudyNode<K1, InMemoryJudyNode<K2, V>> {
    pub fn insert_two_key(&mut self, first_key: &K1, second_key: &K2, value: V) -> Result<()> {
        self.insert_two_key_bytes(
            &bincode::encode_to_vec(first_key, config::standard())?,
            second_key,
            value,
        )
    }

    fn insert_two_key_bytes(
        &mut self,
        first_key_bytes: &[u8],
        second_key: &K2,
        value: V,
    ) -> Result<()> {
        if first_key_bytes.is_empty() {
            let second_key_bytes = bincode::encode_to_vec(second_key, config::standard())?;
            if self.terminal_value.is_none() {
                self.terminal_value = Some(InMemoryJudyNode::default());
            }
            self.terminal_value
                .as_mut()
                .unwrap()
                .insert_bytes(&second_key_bytes, value);
            return Ok(());
        }
        let first_byte = first_key_bytes[0];
        let child = self.children.entry(first_byte).or_default();
        child.insert_two_key_bytes(&first_key_bytes[1..], second_key, value)?;
        Ok(())
    }
}

impl<K, V> InMemoryJudyNode<K, V> {
    fn is_empty(&self) -> bool {
        self.terminal_value.is_none() && self.children.is_empty()
    }
}

impl<K: Key, V: Data> InMemoryJudyNode<K, V> {
    pub fn new() -> Self {
        InMemoryJudyNode {
            children: BTreeMap::new(),
            terminal_value: None,
            _phantom: PhantomData,
        }
    }

    pub async fn to_judy_bytes(&self) -> Result<Bytes> {
        let mut data_bytes: Vec<u8> = vec![];
        let mut root = self.internal_to_judy_node(true, &mut data_bytes).await?;
        root.compress_nodes();
        root.optimize_offsets(0);
        let mut index_bytes = Vec::new();
        root.serialize(&mut index_bytes).await?;
        // TODO: not copy like this
        index_bytes.extend(&data_bytes);
        Ok(Bytes::from(index_bytes))
    }

    #[async_recursion::async_recursion]
    async fn internal_to_judy_node(
        &self,
        is_root: bool,
        mut data_bytes: &mut Vec<u8>,
    ) -> Result<JudyNode> {
        let node_data_offset = data_bytes.len() as u32;
        if self.terminal_value.is_some() {
            let mut terminal_value_bytes =
                bincode::encode_to_vec(self.terminal_value.as_ref().unwrap(), config::standard())?;
            data_bytes
                .write_u32_le(terminal_value_bytes.len() as u32)
                .await?;
            data_bytes.append(&mut terminal_value_bytes);
        }
        let mut judy_node = JudyNode::new();
        judy_node.is_root = is_root;
        for (key, child) in self.children.iter() {
            let child_node = child.internal_to_judy_node(false, &mut data_bytes).await?;
            judy_node.children.insert(*key, child_node);
        }
        judy_node.terminal_offset = self.terminal_value.as_ref().map(|_| node_data_offset);
        Ok(judy_node)
    }

    pub fn to_vec(self) -> Vec<(K, V)> {
        self.to_vec_internal(&[])
    }

    fn to_vec_internal(self, prefix: &[u8]) -> Vec<(K, V)> {
        let mut result = vec![];
        if let Some(terminal_value) = self.terminal_value {
            result.push((
                bincode::decode_from_slice(prefix, config::standard())
                    .unwrap()
                    .0,
                terminal_value,
            ));
        }
        for (key, child) in self.children.into_iter() {
            let mut prefix = prefix.to_vec();
            prefix.push(key);
            result.extend(child.to_vec_internal(&prefix));
        }
        result
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        let key_bytes = bincode::encode_to_vec(k, config::standard()).unwrap();
        self.remove_bytes(&key_bytes)
    }

    pub fn remove_bytes(&mut self, key: &[u8]) -> Option<V> {
        if key.is_empty() {
            return self.terminal_value.take();
        }
        let first_byte = key[0];
        let child = self.children.get_mut(&first_byte)?;
        let result = child.remove_bytes(&key[1..]);
        if child.children.is_empty() && child.terminal_value.is_none() {
            self.children.remove(&first_byte);
        }
        result
    }
}

impl<K1: Send, K2: Key, V: Data> InMemoryJudyNode<K1, InMemoryJudyNode<K2, V>> {
    pub async fn to_judy_bytes(&self) -> Result<Bytes> {
        let mut data_bytes: Vec<u8> = vec![];
        let mut root = self.internal_to_judy_node(true, &mut data_bytes).await?;
        root.compress_nodes();
        root.optimize_offsets(0);
        let mut index_bytes = Vec::new();
        root.serialize(&mut index_bytes).await?;
        // TODO: not copy like this
        index_bytes.extend(&data_bytes);
        Ok(Bytes::from(index_bytes))
    }

    #[async_recursion::async_recursion]
    async fn internal_to_judy_node(
        &self,
        is_root: bool,
        mut data_bytes: &mut Vec<u8>,
    ) -> Result<JudyNode> {
        let node_data_offset = data_bytes.len() as u32;
        if let Some(submap) = &self.terminal_value {
            let mut terminal_value_bytes = submap.to_judy_bytes().await?;
            data_bytes
                .write_u32_le(terminal_value_bytes.len() as u32)
                .await?;
            data_bytes.append(&mut terminal_value_bytes.to_vec());
        }
        let mut judy_node = JudyNode::new();
        judy_node.is_root = is_root;
        for (key, child) in self.children.iter() {
            let child_node = child.internal_to_judy_node(false, &mut data_bytes).await?;
            judy_node.children.insert(*key, child_node);
        }
        judy_node.terminal_offset = self.terminal_value.as_ref().map(|_| node_data_offset);
        Ok(judy_node)
    }
}
