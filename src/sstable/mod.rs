use std::fmt;
use std::fs::*;
use std::io::prelude::*;

#[derive(PartialEq, Debug)]
struct KVPair {
    key: i32,
    value: String,
}

pub struct SSTable {
    c_time: u32,
    pairs: Vec<KVPair>,
}

impl fmt::Display for KVPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(key: {}, value: {})", self.key, self.value)
    }
}

impl fmt::Display for SSTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(len: {}, min: {}, max: {})",
            self.pairs.len(),
            self.pairs[0],
            self.pairs[self.pairs.len() - 1]
        )
    }
}

/* perhaps we need a helper function to generate slice
 * and make the code more graceful.
 */

fn slice_generator(buffer: &Vec<u8>, index: usize) -> [u8; 4] {
    [
        buffer[index],
        buffer[index + 1],
        buffer[index + 2],
        buffer[index + 3],
    ]
}

impl SSTable {
    pub fn new(path: &str) -> Result<Self, std::io::Error> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();

        // read the whole file
        file.read_to_end(&mut buffer)?;

        // now we have the whole buffer and get the metadatas, u8 = 1 bytes, i32 = 4 bytes
        let file_size_slice = slice_generator(&buffer, 0);
        let file_size = u32::from_le_bytes(file_size_slice);

        let time_slice = slice_generator(&buffer, 4);
        let c_time = u32::from_le_bytes(time_slice);

        let nkeys_slice = slice_generator(&buffer, 8);
        let nkeys = u32::from_le_bytes(nkeys_slice);

        // deal with nkeys KVPairs
        let mut pairs = Vec::new();

        let mut now_key: i32;
        let mut now_key_index: u32;
        let mut next_key_index: u32 = 0;

        for i in 0..nkeys - 1 {
            // get the key
            let now_key_slice = slice_generator(&buffer, (12 + 8 * i) as usize);
            now_key = i32::from_le_bytes(now_key_slice);

            // get the index if needed
            if i == 0 {
                let now_key_index_slice = slice_generator(&buffer, (16 + 8 * i) as usize);
                now_key_index = u32::from_le_bytes(now_key_index_slice);
            } else {
                now_key_index = next_key_index;
            }

            // then search for the next key
            let next_key_index_slice = slice_generator(&buffer, (24 + 8 * i) as usize);
            next_key_index = u32::from_le_bytes(next_key_index_slice);

            // get the value
            let now_value =
                String::from_utf8(buffer[now_key_index as usize..next_key_index as usize].to_vec())
                    .unwrap();

            pairs.push(KVPair {
                key: now_key,
                value: now_value,
            })
        }

        // now deal with the last element
        let now_key_slice = slice_generator(&buffer, (12 + 8 * (nkeys - 1)) as usize);
        now_key = i32::from_le_bytes(now_key_slice);

        now_key_index = next_key_index;
        let now_value =
            String::from_utf8(buffer[now_key_index as usize..file_size as usize].to_vec()).unwrap();

        pairs.push(KVPair {
            key: now_key,
            value: now_value,
        });

        assert_eq!(pairs.len(), nkeys as usize);

        Ok(SSTable {
            c_time: c_time,
            pairs: pairs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // a simple basic test to check the creation
    fn test_creation() {
        let test_sstable = SSTable::new("/Users/chrisfisher/lsm-rust/data/sstable-1.sst").unwrap();

        let test_len = test_sstable.pairs.len();

        assert_eq!(test_sstable.pairs.len(), 3);
        // min kv
        assert_eq!(
            test_sstable.pairs[0],
            KVPair {
                key: 1,
                value: "a".to_string()
            }
        );
        // max kv
        assert_eq!(
            test_sstable.pairs[test_len - 1],
            KVPair {
                key: 4,
                value: "d".to_string()
            }
        )
    }
}
