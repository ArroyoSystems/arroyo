use regex::Regex;

pub fn regexp_match(argument: String, regex: String) -> Vec<String> {
    let re = Regex::new(&regex).unwrap();
    match re.captures(argument.as_str()) {
        Some(caps) => caps
            .iter()
            .enumerate()
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

pub fn regexp_replace(argument: String, regex: String, replacement: String) -> String {
    let re = Regex::new(&regex).unwrap();
    let result = re.replace_all(&argument, replacement.as_str());
    result.into_owned()
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::error::Error;

    #[test]
    pub fn test_regexp_match_is_correct() {
        let result = regexp_match(String::from("foobarbequebaz"), String::from("(bar)(beque)"));
        assert_eq!(&result[0], "bar");
        assert_eq!(&result[1], "beque");
    }

    #[test]
    pub fn test_regexp_replace_is_correct() {
        let result = regexp_replace(
            String::from("Thomas"),
            String::from("m|a"),
            String::from("X"),
        );
        assert_eq!(result.as_str(), "ThoXXs");
    }
}
