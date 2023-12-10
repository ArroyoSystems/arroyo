use anyhow::bail;
use regex::Regex;
use serde::de::Visitor;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::sync::OnceLock;
use std::{env, fmt};

#[derive(Debug, Clone)]
pub struct VarStr {
    raw_val: String,
}

impl Serialize for VarStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.raw_val)
    }
}

struct VarStrVisitor;

impl<'de> Visitor<'de> for VarStrVisitor {
    type Value = VarStr;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(VarStr {
            raw_val: value.to_owned(),
        })
    }
}

impl<'de> Deserialize<'de> for VarStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(VarStrVisitor)
    }
}

impl VarStr {
    pub fn new(raw_val: String) -> Self {
        VarStr { raw_val }
    }

    pub fn sub_env_vars(&self) -> anyhow::Result<String> {
        // Regex to match patterns like {{ VAR_NAME }}
        static RE: OnceLock<Regex> = OnceLock::new();
        let re = RE.get_or_init(|| Regex::new(r"\{\{\s*(\w+)\s*}}").unwrap());

        let mut result = self.raw_val.to_string();

        for caps in re.captures_iter(&self.raw_val) {
            let var_name = caps.get(1).unwrap().as_str();
            let full_match = caps.get(0).unwrap().as_str();

            match env::var(var_name) {
                Ok(value) => {
                    result = result.replace(full_match, &value);
                }
                Err(_) => bail!("Environment variable {} not found", var_name),
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_placeholders() {
        let input = "This is a test string with no placeholders";
        assert_eq!(
            VarStr::new(input.to_string()).sub_env_vars().unwrap(),
            input
        );
    }

    #[test]
    fn test_with_placeholders() {
        env::set_var("TEST_VAR", "environment variable");
        let input = "This is a {{ TEST_VAR }}";
        let expected = "This is a environment variable";
        assert_eq!(
            VarStr::new(input.to_string()).sub_env_vars().unwrap(),
            expected
        );
    }

    #[test]
    fn test_multiple_placeholders() {
        env::set_var("VAR1", "first");
        env::set_var("VAR2", "second");
        let input = "Here is the {{ VAR1 }} and here is the {{ VAR2 }}";
        let expected = "Here is the first and here is the second";
        assert_eq!(
            VarStr::new(input.to_string()).sub_env_vars().unwrap(),
            expected
        );
    }
}
