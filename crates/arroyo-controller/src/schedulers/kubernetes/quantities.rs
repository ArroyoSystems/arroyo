use anyhow::{Result, anyhow, bail};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use regex::Regex;
use std::ops::Mul;

#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
enum MemoryUnit {
    m,
    Base,
    Ki,
    Mi,
    Gi,
    Ti,
    Pi,
    k,
    M,
    G,
    T,
    P,
}

const UNITS: [MemoryUnit; 11] = [
    MemoryUnit::P,
    MemoryUnit::Ti,
    MemoryUnit::T,
    MemoryUnit::Gi,
    MemoryUnit::G,
    MemoryUnit::Mi,
    MemoryUnit::M,
    MemoryUnit::Ki,
    MemoryUnit::k,
    MemoryUnit::Base,
    MemoryUnit::m,
];

impl MemoryUnit {
    fn new(unit: &str) -> Result<Self> {
        Ok(match unit {
            "m" => Self::m,
            "Ki" => Self::Ki,
            "Mi" => Self::Mi,
            "Gi" => Self::Gi,
            "Ti" => Self::Ti,
            "Pi" => Self::Pi,
            "k" => Self::k,
            "M" => Self::M,
            "G" => Self::G,
            "T" => Self::T,
            "P" => Self::P,
            "" => Self::Base,
            _ => bail!("invalid memory unit {unit}"),
        })
    }

    pub fn name(&self) -> &'static str {
        match self {
            MemoryUnit::m => "m",
            MemoryUnit::Base => "",
            MemoryUnit::Ki => "Ki",
            MemoryUnit::Mi => "Mi",
            MemoryUnit::Gi => "Gi",
            MemoryUnit::Ti => "Ti",
            MemoryUnit::Pi => "Pi",
            MemoryUnit::k => "k",
            MemoryUnit::M => "M",
            MemoryUnit::G => "G",
            MemoryUnit::T => "T",
            MemoryUnit::P => "P",
        }
    }

    fn multiplier(&self) -> i128 {
        match self {
            MemoryUnit::m => 1,
            MemoryUnit::Base => 1000,
            MemoryUnit::Ki => 1000 * 1024,
            MemoryUnit::Mi => 1000 * 1024 * 1024,
            MemoryUnit::Gi => 1000 * 1024 * 1024 * 1024,
            MemoryUnit::Ti => 1000 * 1024 * 1024 * 1024 * 1024,
            MemoryUnit::Pi => 1000 * 1024 * 1024 * 1024 * 1024 * 1024,
            MemoryUnit::k => 1000 * 1000,
            MemoryUnit::M => 1000 * 1000 * 1000,
            MemoryUnit::G => 1000 * 1000 * 1000 * 1000,
            MemoryUnit::T => 1000 * 1000 * 1000 * 1000 * 1000,
            MemoryUnit::P => 1000 * 1000 * 1000 * 1000 * 1000 * 1000,
        }
    }
}

pub enum ParsedQuantity {
    Cpu(i64),
    Memory(i128),
}

impl ParsedQuantity {
    #[allow(unused)]
    pub fn value(&self) -> i128 {
        match self {
            ParsedQuantity::Cpu(i) => *i as i128,
            ParsedQuantity::Memory(i) => *i,
        }
    }

    pub fn to_canonical(&self) -> String {
        match self {
            ParsedQuantity::Cpu(i) => {
                if *i % 1000 == 0 {
                    format!("{}", i / 1000)
                } else {
                    format!("{i}m")
                }
            }
            ParsedQuantity::Memory(i) => {
                let abs_value = i.abs();
                let neg = if *i < 0 { "-" } else { "" };

                for unit in UNITS {
                    if abs_value % unit.multiplier() == 0 {
                        return format!("{}{}{}", neg, abs_value / unit.multiplier(), unit.name());
                    }
                }

                unreachable!("everything is divisible by 1")
            }
        }
    }
}

impl Mul<i64> for ParsedQuantity {
    type Output = ParsedQuantity;

    fn mul(self, rhs: i64) -> Self::Output {
        match self {
            ParsedQuantity::Cpu(v) => ParsedQuantity::Cpu(v * rhs),
            ParsedQuantity::Memory(v) => ParsedQuantity::Memory(v * rhs as i128),
        }
    }
}

pub trait QuantityParser {
    fn parse_cpu(&self) -> Result<ParsedQuantity>;
    fn parse_memory(&self) -> Result<ParsedQuantity>;
}

const REGEX: &str = r"^(-?[\d\.]+)([a-zA-Z]*)$";

fn parse(s: &str) -> Result<(f64, String)> {
    let regex = Regex::new(REGEX).unwrap();
    let captures = regex
        .captures(s)
        .ok_or_else(|| anyhow!("invalid quantity {}", s))?;
    let quantity: f64 = captures.get(1).unwrap().as_str().parse()?;
    let unit = captures.get(2).unwrap().as_str();
    Ok((quantity, unit.to_string()))
}

impl QuantityParser for Quantity {
    fn parse_cpu(&self) -> Result<ParsedQuantity> {
        let (quantity, unit) = parse(&self.0)?;
        Ok(ParsedQuantity::Cpu(match unit.as_str() {
            "" => (quantity * 1000.0) as i64,
            "m" => quantity as i64,
            _ => bail!("invalid cpu unit '{unit}'"),
        }))
    }

    fn parse_memory(&self) -> Result<ParsedQuantity> {
        let (quantity, unit) = parse(&self.0)?;
        let quantity = (quantity * 1000.0) as i128;
        Ok(ParsedQuantity::Memory(
            quantity * MemoryUnit::new(unit.as_str())?.multiplier() / 1000,
        ))
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

    #[test]
    fn test_parse_cpu_milli() {
        let q = Quantity("500m".to_string());
        assert_eq!(q.parse_cpu().unwrap().value(), 500);
    }

    #[test]
    fn test_parse_cpu_no_unit() {
        let q = Quantity("2".to_string());
        assert_eq!(q.parse_cpu().unwrap().value(), 2000);
    }

    #[test]
    fn test_parse_cpu_invalid_unit() {
        let q = Quantity("2x".to_string());
        assert!(q.parse_cpu().is_err());
    }

    #[test]
    fn test_parse_cpu_negative_value() {
        let q = Quantity("-500m".to_string());
        assert_eq!(q.parse_cpu().unwrap().value(), -500);
    }

    #[test]
    fn test_parse_cpu_with_decimal() {
        let q = Quantity("1.5".to_string());
        assert_eq!(q.parse_cpu().unwrap().value(), 1500);
    }

    #[test]
    fn test_parse_memory_k() {
        let q = Quantity("1k".to_string());
        assert_eq!(q.parse_memory().unwrap().value(), 1_000_000);
    }

    #[test]
    fn test_parse_memory_p() {
        let q = Quantity("1P".to_string());
        assert_eq!(q.parse_memory().unwrap().value(), 1_000_000_000_000_000_000);
    }

    #[test]
    fn test_parse_memory_no_unit() {
        let q = Quantity("500".to_string());
        assert_eq!(q.parse_memory().unwrap().value(), 500_000);
    }

    #[test]
    fn test_parse_memory_millis() {
        let q = Quantity("1m".to_string());
        assert_eq!(q.parse_memory().unwrap().value(), 1);
    }

    #[test]
    fn test_parse_memory_ki() {
        let q = Quantity("1Ki".to_string());
        assert_eq!(q.parse_memory().unwrap().value(), 1_024_000);
    }

    #[test]
    fn test_parse_memory_invalid_unit() {
        let q = Quantity("500x".to_string());
        assert!(q.parse_memory().is_err());
    }

    #[test]
    fn test_parse_memory_negative_value() {
        let q = Quantity("-500Mi".to_string());
        assert_eq!(q.parse_memory().unwrap().value(), -524288000000);
    }

    #[test]
    fn test_parse_memory_with_decimal() {
        let q = Quantity("1.5Gi".to_string());
        assert_eq!(q.parse_memory().unwrap().value(), 1_610_612_736_000);
    }

    #[test]
    fn test_parse_invalid_format() {
        let q = Quantity("invalid".to_string());
        assert!(q.parse_cpu().is_err());
        assert!(q.parse_memory().is_err());
    }

    #[test]
    fn test_parse_empty_string() {
        let q = Quantity("".to_string());
        assert!(q.parse_cpu().is_err());
        assert!(q.parse_memory().is_err());
    }

    #[test]
    fn test_parse_memory_zero_value() {
        let q = Quantity("0".to_string());
        assert_eq!(q.parse_memory().unwrap().value(), 0);
    }

    #[test]
    fn test_parse_cpu_zero_value() {
        let q = Quantity("0".to_string());
        assert_eq!(q.parse_cpu().unwrap().value(), 0);
    }

    #[test]
    fn test_parse_memory_large_value() {
        let q = Quantity("1000T".to_string());
        assert_eq!(q.parse_memory().unwrap().value(), 1_000_000_000_000_000_000);
    }

    fn test_mem(s: &str) {
        let mem = Quantity(s.to_string()).parse_memory().unwrap();
        assert_eq!(s, mem.to_canonical().as_str());
    }

    #[test]
    fn test_to_canonical() {
        test_mem("10T");
        test_mem("100k");
        test_mem("-55P");
        test_mem("50");
        test_mem("100Ki");
    }
}
