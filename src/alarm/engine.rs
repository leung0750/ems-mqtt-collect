use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Operator {
    LessEqual,
    Less,
    GreaterEqual,
    Greater,
    NotEqual,
    Equal,
}

impl FromStr for Operator {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "≤" | "<=" => Ok(Operator::LessEqual),
            "<" => Ok(Operator::Less),
            "≥" | ">=" => Ok(Operator::GreaterEqual),
            ">" => Ok(Operator::Greater),
            "≠" | "!=" => Ok(Operator::NotEqual),
            "=" | "==" => Ok(Operator::Equal),
            _ => Err(format!("未知的运算符: {}", s)),
        }
    }
}

impl Operator {
    pub fn description(&self) -> &'static str {
        match self {
            Operator::LessEqual => "不大于",
            Operator::Less => "低于",
            Operator::GreaterEqual => "不小于",
            Operator::Greater => "超过",
            Operator::NotEqual => "不等于",
            Operator::Equal => "等于",
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AlarmConfig {
    pub id: i64,
    pub field_name: String,
    pub operator: Operator,
    pub threshold: f64,
}

impl AlarmConfig {
    pub fn is_triggered(&self, current_value: f64) -> bool {
        match self.operator {
            Operator::LessEqual => current_value <= self.threshold,
            Operator::Less => current_value < self.threshold,
            Operator::GreaterEqual => current_value >= self.threshold,
            Operator::Greater => current_value > self.threshold,
            Operator::NotEqual => (current_value - self.threshold).abs() > f64::EPSILON,
            Operator::Equal => (current_value - self.threshold).abs() <= f64::EPSILON,
        }
    }

    pub fn operator_desc(&self) -> &'static str {
        self.operator.description()
    }
}

impl TryFrom<super::AlarmConfigData> for AlarmConfig {
    type Error = String;

    fn try_from(data: super::AlarmConfigData) -> Result<Self, Self::Error> {
        let operator = Operator::from_str(&data.operator)?;
        Ok(AlarmConfig {
            id: data.id,
            field_name: data.alarm_field,
            operator,
            threshold: data.threshold,
        })
    }
}

#[allow(dead_code)]
pub fn check_alarm(config: &AlarmConfig, current_value: f64) -> bool {
    config.is_triggered(current_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_from_str() {
        assert_eq!(Operator::from_str("≤").unwrap(), Operator::LessEqual);
        assert_eq!(Operator::from_str("<=").unwrap(), Operator::LessEqual);
        assert_eq!(Operator::from_str("<").unwrap(), Operator::Less);
        assert_eq!(Operator::from_str("≥").unwrap(), Operator::GreaterEqual);
        assert_eq!(Operator::from_str(">=").unwrap(), Operator::GreaterEqual);
        assert_eq!(Operator::from_str(">").unwrap(), Operator::Greater);
        assert_eq!(Operator::from_str("≠").unwrap(), Operator::NotEqual);
        assert_eq!(Operator::from_str("!=").unwrap(), Operator::NotEqual);
        assert_eq!(Operator::from_str("=").unwrap(), Operator::Equal);
        assert_eq!(Operator::from_str("==").unwrap(), Operator::Equal);
    }

    #[test]
    fn test_is_triggered_greater() {
        let config = AlarmConfig {
            id: 1,
            field_name: "pw".to_string(),
            operator: Operator::Greater,
            threshold: 500.0,
        };
        assert!(config.is_triggered(550.0));
        assert!(!config.is_triggered(500.0));
        assert!(!config.is_triggered(450.0));
    }

    #[test]
    fn test_is_triggered_less() {
        let config = AlarmConfig {
            id: 1,
            field_name: "tp".to_string(),
            operator: Operator::Less,
            threshold: 10.0,
        };
        assert!(config.is_triggered(5.0));
        assert!(!config.is_triggered(10.0));
        assert!(!config.is_triggered(15.0));
    }
}
