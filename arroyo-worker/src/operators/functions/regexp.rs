use core::panic;

use arrow_string;
use regex::Regex;

pub fn regexp_match(argument: Vec<u8>, regex: Regex) -> Result<Boolean,Box<dyn Error>> {
    panic!("Not implemented")
}

https://github.com/apache/arrow-rs/blob/master/arrow-string/src/regexp.rs