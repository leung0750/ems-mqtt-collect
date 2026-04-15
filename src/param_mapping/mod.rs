use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::{Lazy, OnceCell};
use sqlx::Row;
use tokio::sync::RwLock;

static PARAM_MAPPING_CACHE: OnceCell<Arc<RwLock<ParamMappingCache>>> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct ParamMappingCache {
    field_to_cn: HashMap<String, String>,
    alias_to_field: HashMap<String, String>,
}

impl ParamMappingCache {
    fn new() -> Self {
        let field_to_cn = HashMap::from([
            ("ua".to_string(), "A相电压".to_string()),
            ("ub".to_string(), "B相电压".to_string()),
            ("uc".to_string(), "C相电压".to_string()),
            ("uab".to_string(), "AB线电压".to_string()),
            ("ubc".to_string(), "BC线电压".to_string()),
            ("uac".to_string(), "AC线电压".to_string()),
            ("ia".to_string(), "A相电流".to_string()),
            ("ib".to_string(), "B相电流".to_string()),
            ("ic".to_string(), "C相电流".to_string()),
            ("z_i".to_string(), "零序电流".to_string()),
            ("pa".to_string(), "A相有功功率".to_string()),
            ("pb".to_string(), "B相有功功率".to_string()),
            ("pc".to_string(), "C相有功功率".to_string()),
            ("p".to_string(), "总有功功率".to_string()),
            ("q".to_string(), "总无功功率".to_string()),
            ("s".to_string(), "总视在功率".to_string()),
            ("fa".to_string(), "A相功率因数".to_string()),
            ("fb".to_string(), "B相功率因数".to_string()),
            ("fc".to_string(), "C相功率因数".to_string()),
            ("f".to_string(), "总功率因数".to_string()),
            ("hz".to_string(), "频率".to_string()),
            ("pw".to_string(), "正向有功电能".to_string()),
            ("pw_fan".to_string(), "反向有功电能".to_string()),
            ("qw".to_string(), "正向无功电能".to_string()),
            ("qw_fan".to_string(), "反向无功电能".to_string()),
            ("sw".to_string(), "视在电能".to_string()),
            ("peak_plus_energy".to_string(), "尖峰时段能耗".to_string()),
            ("peak_energy".to_string(), "高峰时段能耗".to_string()),
            ("flat_energy".to_string(), "平时段能耗".to_string()),
            ("valley_energy".to_string(), "谷时段能耗".to_string()),
            ("st".to_string(), "累计流量".to_string()),
            ("st_raw".to_string(), "原始累计量".to_string()),
            ("fr".to_string(), "瞬时流量".to_string()),
            ("sf".to_string(), "标况流量".to_string()),
            ("wf".to_string(), "工况流量".to_string()),
            ("tp".to_string(), "温度".to_string()),
            ("pr".to_string(), "压力".to_string()),
            ("dx".to_string(), "X轴位移".to_string()),
            ("dy".to_string(), "Y轴位移".to_string()),
            ("dz".to_string(), "Z轴位移".to_string()),
            ("vx".to_string(), "X轴速度".to_string()),
            ("vy".to_string(), "Y轴速度".to_string()),
            ("vz".to_string(), "Z轴速度".to_string()),
        ]);

        let mut alias_to_field = HashMap::new();
        for aliases in FALLBACK_ALIASES.iter() {
            for alias in &aliases.1 {
                alias_to_field.insert((*alias).to_string(), aliases.0.to_string());
            }
        }

        Self {
            field_to_cn,
            alias_to_field,
        }
    }

    pub fn get_chinese_name(&self, field: &str) -> String {
        self.field_to_cn
            .get(field)
            .cloned()
            .unwrap_or_else(|| field.to_string())
    }

    pub fn get_standard_field(&self, alias: &str) -> Option<String> {
        self.alias_to_field.get(alias).cloned()
    }

    pub fn update_from_db(&mut self, rows: Vec<(String, String)>) {
        for (field_key, field_name_cn) in rows {
            self.field_to_cn.insert(field_key.clone(), field_name_cn);
            self.alias_to_field
                .insert(field_key.clone(), field_key.clone());
            self.alias_to_field
                .insert(field_key.to_uppercase(), field_key.clone());
            if let Some(field_name_cn) = self.field_to_cn.get(&field_key) {
                self.alias_to_field
                    .insert(field_name_cn.clone(), field_key.clone());
            }
        }
    }
}

static FALLBACK_ALIASES: Lazy<Vec<(&'static str, Vec<&'static str>)>> = Lazy::new(|| {
    vec![
        ("ua", vec!["ua", "UA", "Ua"]),
        ("ub", vec!["ub", "UB", "Ub"]),
        ("uc", vec!["uc", "UC", "Uc"]),
        ("avg_u", vec!["avg_u", "AVG_U"]),
        ("uab", vec!["uab", "UAB", "Uab"]),
        ("ubc", vec!["ubc", "UBC", "Ubc"]),
        ("uac", vec!["uac", "UAC", "Uac"]),
        ("avg_uu", vec!["avg_uu", "AVG_UU"]),
        ("ia", vec!["ia", "IA", "Ia"]),
        ("ib", vec!["ib", "IB", "Ib"]),
        ("ic", vec!["ic", "IC", "Ic"]),
        ("avg_i", vec!["avg_i", "AVG_I"]),
        ("z_i", vec!["z_i", "ZI"]),
        ("pa", vec!["pa", "PA"]),
        ("pb", vec!["pb", "PB"]),
        ("pc", vec!["pc", "PC"]),
        ("p", vec!["p", "P", "三相有功功率"]),
        ("q", vec!["q", "Q"]),
        ("s", vec!["s", "S"]),
        ("fa", vec!["fa", "FA", "PFA"]),
        ("fb", vec!["fb", "FB", "PFB"]),
        ("fc", vec!["fc", "FC", "PFC"]),
        ("f", vec!["f", "F", "PFS"]),
        ("hz", vec!["hz", "HZ"]),
        ("pw", vec!["pw", "PW", "EP"]),
        ("pw_fan", vec!["pw_fan", "PW_FAN"]),
        ("qw", vec!["qw", "QW"]),
        ("qw_fan", vec!["qw_fan", "QW_FAN"]),
        ("sw", vec!["sw", "SW"]),
        ("peak_plus_energy", vec!["peak_plus_energy"]),
        ("peak_energy", vec!["peak_energy"]),
        ("flat_energy", vec!["flat_energy"]),
        ("valley_energy", vec!["valley_energy"]),
        (
            "st",
            vec![
                "st",
                "ST",
                "标况体积总量",
                "风量",
                "累计流量",
                "用水量",
                "累计质量流量",
            ],
        ),
        ("st_raw", vec!["st_raw", "ST_RAW"]),
        ("fr", vec!["fr", "FR", "瞬时流量", "风速", "瞬时质量流量"]),
        ("sf", vec!["sf", "SF", "标况流量"]),
        ("wf", vec!["wf", "WF", "工况流量", "补偿后流量"]),
        ("tp", vec!["tp", "TP", "温度"]),
        ("pr", vec!["pr", "PR", "压力"]),
        ("dx", vec!["dx", "DX"]),
        ("dy", vec!["dy", "DY"]),
        ("dz", vec!["dz", "DZ"]),
        ("vx", vec!["vx", "VX"]),
        ("vy", vec!["vy", "VY"]),
        ("vz", vec!["vz", "VZ"]),
    ]
});

static FALLBACK_MAPPING: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut map = HashMap::new();
    for (field, aliases) in FALLBACK_ALIASES.iter() {
        for alias in aliases {
            map.insert(*alias, *field);
        }
    }
    map
});

pub async fn init_param_mapping() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut cache = ParamMappingCache::new();

    let pool = crate::mysqldb::get_pool().await?;
    let sql = "SELECT field_key, field_name_cn FROM param_mapping";
    let result = sqlx::query(sql).fetch_all(&pool).await;

    match result {
        Ok(rows) => {
            let db_rows: Vec<(String, String)> = rows
                .into_iter()
                .map(|row| {
                    let field_key: String = row.get("field_key");
                    let field_name_cn: String = row.get("field_name_cn");
                    (field_key, field_name_cn)
                })
                .collect();
            cache.update_from_db(db_rows);
            println!("loaded {} param mappings", cache.field_to_cn.len());
        }
        Err(err) => {
            eprintln!(
                "load param mappings from mysql failed, using fallback: {}",
                err
            );
        }
    }

    let cache = Arc::new(RwLock::new(cache));
    let _ = PARAM_MAPPING_CACHE.set(cache);
    Ok(())
}

fn get_cache() -> Option<Arc<RwLock<ParamMappingCache>>> {
    PARAM_MAPPING_CACHE.get().cloned()
}

pub async fn get_chinese_name(field: &str) -> String {
    if let Some(cache) = get_cache() {
        let cache = cache.read().await;
        cache.get_chinese_name(field)
    } else {
        field.to_string()
    }
}

pub async fn get_standard_field(alias: &str) -> Option<String> {
    if let Some(cache) = get_cache() {
        let cache = cache.read().await;
        cache.get_standard_field(alias)
    } else {
        FALLBACK_MAPPING
            .get(alias)
            .map(|value| (*value).to_string())
    }
}

pub fn get_param_mapping_sync() -> &'static HashMap<&'static str, &'static str> {
    &FALLBACK_MAPPING
}

#[cfg(test)]
mod tests {
    use super::ParamMappingCache;

    #[test]
    fn maps_three_phase_active_power_to_p() {
        let cache = ParamMappingCache::new();

        assert_eq!(
            cache.get_standard_field("三相有功功率"),
            Some("p".to_string())
        );
    }
}
