use std::{
    borrow::Cow,
    io::{Cursor, Read, Seek, SeekFrom},
};

use crate::judy::{BytesWithOffset, ChildrenDensity, HasCommonBytes, JudyNode, JudyNodeHeader};
use anyhow::Result;
use arroyo_types::Key;
use bincode::{config, de::Decoder, BorrowDecode, Decode};
use byteorder::ReadBytesExt;
use bytes::Bytes;
use tracing::warn;

pub struct JudyReader<K: Key, V> {
    index_cursor: Cursor<Bytes>,
    data_cursor: BytesWithOffset,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K: Key, V> JudyReader<K, V> {
    pub fn new(bytes: Bytes) -> Result<Self> {
        let mut index_cursor = Cursor::new(bytes.clone());
        let header_byte = index_cursor.read_u8()?;
        let header = JudyNodeHeader::try_from(header_byte)?;
        let header_size = header
            .offset_contribution_size
            .read_u32_sync(&mut index_cursor)?
            .ok_or_else(|| anyhow::anyhow!("No header size"))?;
        warn!("byte size: {}, header size: {}", bytes.len(), header_size);
        index_cursor.set_position(0);
        let data_cursor = BytesWithOffset::new(bytes);

        Ok(Self {
            index_cursor,
            data_cursor,
            _phantom: std::marker::PhantomData,
        })
    }
    pub fn get_bytes(&mut self, key: &K) -> Result<Option<Bytes>> {
        self.index_cursor.seek(SeekFrom::Start(0))?;
        self.data_cursor.seek(SeekFrom::Start(0))?;

        self.get_bytes_internal(bincode::encode_to_vec(key, config::standard())?.as_slice())
    }
    fn get_bytes_internal(&mut self, mut key: &[u8]) -> Result<Option<Bytes>> {
        let starting_position = self.index_cursor.position();

        let header_byte = self.index_cursor.read_u8()?;
        let header = JudyNodeHeader::try_from(header_byte)?;

        if header.has_common_bytes == HasCommonBytes::Yes {
            let common_len = self.index_cursor.read_u8()?;
            let mut common_bytes = vec![0u8; common_len as usize];
            self.index_cursor.read_exact(&mut common_bytes)?;
            if !key.starts_with(&common_bytes) {
                return Ok(None);
            }
            key = &key[common_bytes.len()..];
        }
        let offset_contribution = header
            .offset_contribution_size
            .read_u32_sync(&mut self.index_cursor)?;
        if let Some(offset_contribution) = offset_contribution {
            self.data_cursor
                .seek(SeekFrom::Current(offset_contribution as i64))?;
        }
        if key.is_empty() {
            if header.has_terminal_value() {
                let value_size = self.data_cursor.read_u32()?;
                return Ok(Some(self.data_cursor.read_next_n_bytes(value_size)?));
            } else {
                return Ok(None);
            }
        }
        let next_byte = key[0];
        key = &key[1..];
        let Some(child_offset) = self.get_child_offset(next_byte, &header)? else {
            return Ok(None);
        };
        if child_offset <= 0 {
            self.data_cursor
                .seek(SeekFrom::Current(-child_offset as i64))?;
            let value_size = self.data_cursor.read_u32()?;
            Ok(Some(self.data_cursor.read_next_n_bytes(value_size)?))
        } else {
            self.index_cursor
                .seek(SeekFrom::Start(starting_position + child_offset as u64))?;
            self.get_bytes_internal(key)
        }
    }

    fn get_child_offset(&mut self, byte: u8, header: &JudyNodeHeader) -> Result<Option<i32>> {
        match header.children_density {
            ChildrenDensity::Sparse => {
                let key_count = (self.index_cursor.read_u8()? as usize) + 1;
                // scan key_count, looking for bytes
                for i in 0..key_count {
                    let key = self.index_cursor.read_u8()?;
                    if key == byte {
                        self.index_cursor.seek(SeekFrom::Current(
                            ((key_count - i - 1) + header.child_pointer_size.byte_count() * i)
                                as i64,
                        ))?;
                        return Ok(header
                            .child_pointer_size
                            .read_i32_sync(&mut self.index_cursor)?);
                    } else if byte < key {
                        return Ok(None);
                    }
                }
                Ok(None)
            }
            ChildrenDensity::Dense => {
                // advance key/8
                let mut bitmap = [0u8; 32];
                self.index_cursor.read_exact(&mut bitmap)?;
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
                self.index_cursor.seek(SeekFrom::Current(
                    (rank * header.child_pointer_size.byte_count()) as i64,
                ))?;
                Ok(header
                    .child_pointer_size
                    .read_i32_sync(&mut self.index_cursor)?)
            }
        }
    }
}

impl<K: Key, V: Decode> JudyReader<K, V> {
    pub fn as_vec(&mut self) -> Result<Vec<(K, V)>> {
        self.index_cursor.seek(SeekFrom::Start(0))?;
        self.data_cursor.seek(SeekFrom::Start(0))?;
        self.as_vec_internal(vec![])
    }
    pub fn get(&mut self, key: &K) -> Result<Option<V>> {
        self.get_bytes(key)?
            .map(|bytes| Ok(bincode::decode_from_slice(&bytes, config::standard())?.0))
            .transpose()
    }
    fn as_vec_internal(&mut self, mut prefix: Vec<u8>) -> Result<Vec<(K, V)>> {
        let initial_index_offset = self.index_cursor.position();
        let header_byte = self.index_cursor.read_u8()?;
        let header = JudyNodeHeader::try_from(header_byte)?;
        let mut result = vec![];
        if header.has_common_bytes == HasCommonBytes::Yes {
            let common_len = self.index_cursor.read_u8()?;
            let mut common_bytes = vec![0u8; common_len as usize];
            self.index_cursor.read_exact(&mut common_bytes)?;
            // extend prefix
            prefix.extend(common_bytes);
        }
        let offset_contribution = header
            .offset_contribution_size
            .read_u32_sync(&mut self.index_cursor)?;
        if let Some(offset_contribution) = offset_contribution {
            self.data_cursor
                .seek(SeekFrom::Current(offset_contribution as i64))?;
        }
        let initial_data_offset = self.data_cursor.offset();
        if header.has_terminal_value() {
            let value_size = self.data_cursor.read_u32()?;
            let value = self.data_cursor.read_next_n_bytes(value_size)?;
            result.push((
                bincode::decode_from_slice(&prefix, config::standard())?.0,
                bincode::decode_from_slice(&value, config::standard())?.0,
            ));
        }
        if header.child_pointer_size.byte_count() == 0 {
            return Ok(result);
        }
        let (keys, offsets) = match header.children_density {
            ChildrenDensity::Sparse => {
                let key_count = (self.index_cursor.read_u8()? as usize) + 1;
                let mut keys = vec![];
                let mut offsets = vec![];
                for _ in 0..key_count {
                    let key = self.index_cursor.read_u8()?;
                    keys.push(key);
                    offsets.push(
                        header
                            .child_pointer_size
                            .read_i32_sync(&mut self.index_cursor)?
                            .unwrap(),
                    );
                }
                (keys, offsets)
            }
            ChildrenDensity::Dense => {
                let mut bitmap = [0u8; 32];
                self.index_cursor.read_exact(&mut bitmap)?;
                let mut keys = vec![];
                let mut offsets = vec![];
                for i in 0..bitmap.len() {
                    for j in 0..8 {
                        if bitmap[i] & (1 << j) != 0 {
                            keys.push((i * 8 + j) as u8);
                            offsets.push(
                                header
                                    .child_pointer_size
                                    .read_i32_sync(&mut self.index_cursor)?
                                    .unwrap(),
                            );
                        }
                    }
                }
                (keys, offsets)
            }
        };
        for (key, offset) in keys.into_iter().zip(offsets.into_iter()) {
            prefix.push(key);
            if offset <= 0 {
                self.data_cursor.seek(SeekFrom::Start(
                    (initial_data_offset as i64 - key as i64) as u64,
                ))?;
                let value_size = self.data_cursor.read_u32()?;
                let value = self.data_cursor.read_next_n_bytes(value_size)?;
                result.push((
                    bincode::decode_from_slice(&prefix, config::standard())?.0,
                    bincode::decode_from_slice(&value, config::standard())?.0,
                ));
            } else {
                self.index_cursor
                    .seek(SeekFrom::Start(initial_index_offset + offset as u64))?;
                result.extend(self.as_vec_internal(prefix.clone())?);
            }
            prefix.pop();
        }

        Ok(result)
    }
}

impl<K1: Key, K2: Key, V> JudyReader<K1, JudyReader<K2, V>> {
    pub fn lookup_pair_bytes(&mut self, key1: &K1, key2: &K2) -> Result<Option<Bytes>> {
        let Some(mut submap): Option<JudyReader<K2, V>> = self
            .get_bytes(key1)?
            .map(|bytes| JudyReader::new(bytes))
            .transpose()?
        else {
            return Ok(None);
        };
        submap.get_bytes(key2)
    }
}

impl<K1: Key, K2: Key, V: Decode> JudyReader<K1, JudyReader<K2, V>> {
    pub fn get_pair(&mut self, key1: &K1, key2: &K2) -> Result<Option<V>> {
        let Some(bytes) = self.lookup_pair_bytes(key1, key2)? else {
            return Ok(None);
        };
        Ok(Some(
            bincode::decode_from_slice(&bytes, config::standard())?.0,
        ))
    }
}
