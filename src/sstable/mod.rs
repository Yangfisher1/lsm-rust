use std::fs::*;
use std::io::prelude::*;

struct KVPair {
    key: i32,
    value: String,
}

struct SSTable {
    c_time: u32,
    pairs: Vec<KVPair>,
}

impl SSTable {
    fn new(path: &str) -> Result<Self, std::io::Error> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();

        // read the whole file
        file.read_to_end(&mut buffer)?;

        // now we have the whole buffer and get the metadatas, u8 = 1 bytes, i32 = 4 bytes
        let file_size_slice: [u8; 4] = [buffer[0], buffer[1], buffer[2], buffer[3]];
        let file_size = u32::from_le_bytes(file_size_slice);

        let time_slice: [u8; 4] = [buffer[4], buffer[5], buffer[6], buffer[7]];
        let c_time = u32::from_le_bytes(time_slice);

        let nkeys_slice: [u8; 4] = [buffer[8], buffer[9], buffer[10], buffer[11]];
        let nkeys = u32::from_le_bytes(nkeys_slice);

        // deal with nkeys KVPairs
        let mut pairs = Vec::new();

        let mut now_key : i32 = 0;
        let mut now_key_index : u32 = 0;
        let mut next_key_index : u32 = 0;
        let mut value_len : u32 = 0;

        for i in 0..nkeys - 1 {
            // get the key
            let now_key_slice: [u8; 4] = [
                buffer[(12 + 8 * i) as usize],
                buffer[(13 + 8 * i) as usize],
                buffer[(14 + 8 * i) as usize],
                buffer[(15 + 8 * i) as usize],
            ];
            now_key = i32::from_le_bytes(now_key_slice);

            // get the index if needed
            if i == 0 {
                let now_key_index_slice: [u8; 4] = [
                    buffer[(16 + 8 * i) as usize],
                    buffer[(17 + 8 * i) as usize],
                    buffer[(18 + 8 * i) as usize],
                    buffer[(19 + 8 * i) as usize],
                ];
                now_key_index = u32::from_le_bytes(now_key_index_slice);
            } else {
                now_key_index = next_key_index; 
            }

            // then search for the next key
            let next_key_index_slice: [u8; 4] = [
                buffer[(24 + 8 * i) as usize],
                buffer[(25 + 8 * i) as usize],
                buffer[(26 + 8 * i) as usize],
                buffer[(27 + 8 * i) as usize],
            ];
            next_key_index = u32::from_le_bytes(next_key_index_slice);

            // get the value
            value_len = next_key_index - now_key_index;
            let now_value = String::from_utf8(buffer[now_key_index as usize..next_key_index as usize].to_vec()).unwrap();

            pairs.push(KVPair {
                key: now_key,
                value: now_value,
            })
        }

        Ok(SSTable {
            c_time: c_time,
            pairs: Vec::new(),
        })
    }
}
