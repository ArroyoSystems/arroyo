use regex::Regex;

pub fn regexp_match(argument: String, re: Regex) -> Vec<String> {
    match re.captures(argument.as_str()) {
        Some(caps) => caps.iter().enumerate()
            .filter_map(|(i, cap)| {
                if i != 0 {
                    cap.map(|m| m.as_str().to_owned())
                } else {
                    None
                }
            })
            .collect(),
        None => vec![],
    }
}


pub fn regexp_replace(argument: String, re: Regex, replacement: String, start: usize, n: usize) -> String {
    let (pre_start, from_start) = argument.split_at(start);
    let mut result = from_start.to_string();

    // Collect the spans of all matches
    let spans: Vec<_> = re.find_iter(&result)
        .map(|m| m.start()..m.end())
        .collect();

    if n > 0 {
        // Only replace the `n`th occurrence if it exists
        if let Some(range) = spans.get(n - 2) {
            result.replace_range(range.clone(), &replacement);
        }
    } else {
        // Replace all occurrences, iterating in reverse order to not invalidate the spans
        for range in spans.into_iter().rev() {
            result.replace_range(range, &replacement);
        }
    }

    format!("{}{}", pre_start, result)
}
#[cfg(test)]
mod tests{

    use super::*;
    use std::error::Error;

    #[test]
    pub fn test_regexp_match_is_correct() -> Result<(),Box<dyn Error>>{
        let regex = Regex::new("(bar)(beque)")?;
        let result = regexp_match(String::from("foobarbequebaz"), regex);
        assert_eq!(&result[0],"bar");
        assert_eq!(&result[1],"beque");
        Ok(())
    }

    #[test]
    pub fn test_regexp_replace_is_correct() -> Result<(),Box<dyn Error>>{
        let regex = Regex::new(".")?;
        let result = regexp_replace(String::from("Thomas"), regex, String::from("X"),3,2);
        assert_eq!(result.as_str(), "ThoXas");
        Ok(())
    }
    #[test]
    pub fn test_regexp_replace_replaces_all() -> Result<(),Box<dyn Error>>{
        let regex = Regex::new(".")?;
        let result = regexp_replace(String::from("Thomas"), regex, String::from("X"),3,0);
        assert_eq!(result.as_str(), "ThoXXX");
        Ok(())
    }
    
}