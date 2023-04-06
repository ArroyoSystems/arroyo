pub mod strings {
    pub fn ascii(argument: String) -> i32 {
        argument
            .chars()
            .next()
            .map(|char| char as i32)
            .unwrap_or_default()
    }

    pub fn bit_length(argument: String) -> i32 {
        argument.len() as i32 * 8
    }

    pub fn trim(string: String, trimming_characters: String) -> String {
        let chars: Vec<char> = trimming_characters.chars().collect();
        let char_slice: &[char] = &chars;
        string.trim_matches(char_slice).to_string()
    }

    pub fn char_length(argument: String) -> i32 {
        length(argument)
    }

    pub fn chr(argument: i64) -> String {
        std::char::from_u32(argument as u32)
            .unwrap_or_default()
            .to_string()
    }

    pub fn initcap(argument: String) -> String {
        let mut result = String::with_capacity(argument.len());
        let mut new_word = true;

        for c in argument.chars() {
            if c.is_alphanumeric() {
                if new_word {
                    result.extend(c.to_uppercase());
                    new_word = false;
                } else {
                    result.extend(c.to_lowercase());
                }
            } else {
                new_word = true;
                result.push(c);
            }
        }
        result
    }

    pub fn left(s: String, n: i64) -> String {
        if n < 0 {
            s.chars()
                .take(s.chars().count().saturating_sub(n.abs() as usize))
                .collect()
        } else {
            s.chars().take(n as usize).collect()
        }
    }

    pub fn length(s: String) -> i32 {
        s.chars().count() as i32
    }

    pub fn lpad(s: String, length: usize, fill: String) -> String {
        let s_len = s.chars().count();
        if s_len == length {
            return s;
        }
        if s_len > length {
            return s.chars().take(length).collect();
        }
        let fill_chars: Vec<char> = fill.chars().collect();
        let mut result = String::new();

        if length > s_len {
            for i in 0..(length - s_len) {
                result.push(fill_chars[i % fill_chars.len()]);
            }
        }

        result.push_str(&s);
        result
    }

    pub fn ltrim(string: String, trimming_characters: String) -> String {
        let chars: Vec<char> = trimming_characters.chars().collect();
        let char_slice: &[char] = &chars;
        string.trim_start_matches(char_slice).to_string()
    }

    pub fn split_part(s: String, delimiter: String, n: isize) -> String {
        let parts: Vec<&str> = s.split(&delimiter).collect();

        if n > 0 {
            parts
                .get((n - 1) as usize)
                .cloned()
                .unwrap_or("")
                .to_string()
        } else {
            parts
                .get(parts.len().saturating_sub((-n) as usize))
                .cloned()
                .unwrap_or("")
                .to_string()
        }
    }

    pub fn starts_with(s: String, prefix: String) -> bool {
        s.starts_with(&prefix)
    }

    pub fn strpos(s: String, substring: String) -> usize {
        s.find(&substring).map_or(0, |index| index + 1)
    }

    pub fn substr(s: String, start: usize, count: Option<usize>) -> String {
        let start_index = start.saturating_sub(1);
        match count {
            Some(count) => s.chars().skip(start_index).take(count).collect(),
            None => s.chars().skip(start_index).collect(),
        }
    }

    pub fn translate(s: String, from: String, to: String) -> String {
        let from_chars: Vec<char> = from.chars().collect();
        let to_chars: Vec<char> = to.chars().collect();
        let mut translation_map = std::collections::HashMap::new();

        for (i, from_char) in from_chars.iter().enumerate() {
            let to_char = to_chars.get(i).copied();
            translation_map.insert(from_char, to_char);
        }

        s.chars()
            .filter_map(|c| {
                if let Some(mapped_char) = translation_map.get(&c) {
                    if mapped_char.is_none() {
                        None // Delete the character
                    } else {
                        *mapped_char
                    }
                } else {
                    Some(c)
                }
            })
            .collect()
    }

    pub fn octet_length(s: String) -> usize {
        s.as_bytes().len()
    }

    pub fn right(s: String, n: isize) -> String {
        if n < 0 {
            s.chars().skip(n.abs() as usize).collect()
        } else {
            s.chars()
                .rev()
                .take(n as usize)
                .collect::<String>()
                .chars()
                .rev()
                .collect()
        }
    }

    pub fn rpad(s: String, length: usize, fill: String) -> String {
        let s_len = s.chars().count();
        if s_len == length {
            return s;
        }
        if s_len > length {
            return s.chars().take(length).collect();
        }
        let fill_chars: Vec<char> = fill.chars().collect();
        let mut result = s;

        for i in 0..(length.saturating_sub(s_len)) {
            result.push(fill_chars[i % fill_chars.len()]);
        }
        result
    }
    pub fn rtrim(string: String, trimming_characters: String) -> String {
        let chars: Vec<char> = trimming_characters.chars().collect();
        let char_slice: &[char] = &chars;
        string.trim_end_matches(char_slice).to_string()
    }
}
