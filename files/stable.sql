/* 1. 创建数据库 (如果不存在) */
/* KEEP 3650: 数据保留 10 年 */
/* UPDATE 1: 允许更新数据 (修正采集错误) */
-- CREATE DATABASE IF NOT EXISTS iot_data PRECISION 'ms' KEEP 3650;

-- USE iot_data;

/* 2. 创建超级表 device_metrics */
CREATE STABLE IF NOT EXISTS collect_energy (
    /* === 基础字段 === */
    ts TIMESTAMP,              -- [时间戳] 数据采集时间 (主键)

    /* === A. 电力：电压 (FLOAT) === */
    ua FLOAT,                  -- [A相电压] (V)
    ub FLOAT,                  -- [B相电压] (V)
    uc FLOAT,                  -- [C相电压] (V)
    uab FLOAT,                 -- [AB线电压] (V)
    ubc FLOAT,                 -- [BC线电压] (V)
    uac FLOAT,                 -- [AC线电压] (V)

    /* === B. 电力：电流 (FLOAT) === */
    ia FLOAT,                  -- [A相电流] (A)
    ib FLOAT,                  -- [B相电流] (A)
    ic FLOAT,                  -- [C相电流] (A)
    z_i FLOAT,                 -- [零序电流] (A) 漏电监测用

    /* === C. 电力：功率 (FLOAT) === */
    pa FLOAT,                  -- [A相有功功率] (kW)
    pb FLOAT,                  -- [B相有功功率] (kW)
    pc FLOAT,                  -- [C相有功功率] (kW)
    p  FLOAT,                  -- [总有功功率] (kW) 包含逆变器输出
    q  FLOAT,                  -- [总无功功率] (kVar)
    s  FLOAT,                  -- [总视在功率] (kVA)

    /* === D. 电力：因数与频率 (FLOAT) === */
    fa FLOAT,                  -- [A相功率因数] (0-1)
    fb FLOAT,                  -- [B相功率因数] (0-1)
    fc FLOAT,                  -- [C相功率因数] (0-1)
    f  FLOAT,                  -- [总功率因数] (0-1)
    hz FLOAT,                  -- [电网频率] (Hz)

    /* === E. 电力：电能抄表 (DOUBLE - 核心计费) === */
    /* ⚠️注意：电能数值大且累加，必须用 DOUBLE 防止精度丢失 */
    pw DOUBLE,                 -- [正向有功总电能] (kWh) 类似“总发电量”
    pw_fan DOUBLE,             -- [反向有功总电能] (kWh) 上网/回馈电量
    qw DOUBLE,                 -- [正向无功总电能] (kVarh)
    qw_fan DOUBLE,             -- [反向无功总电能] (kVarh)
    sw DOUBLE,                 -- [总视在电能] (kVAh)

    /* === F. 流体：水/气/热 (兼容设计) === */
    st DOUBLE,                 -- [结算累计量] (水=m³, 气=Nm³, 蒸汽=t) 核心计费读数
    st_raw DOUBLE,             -- [原始累计量] (可选) 未经温压补偿的原始读数
    
    fr FLOAT,                  -- [通用瞬时流速] (通用单位) 
    sf FLOAT,                  -- [标况流量] (Nm³/h) 天然气专用
    wf FLOAT,                  -- [工况流量] (m³/h) 管道实际流速
    
    tp FLOAT,                  -- [流体温度] (℃)
    pr FLOAT,                  -- [流体压力] (MPa/kPa)

    /* === G. 机械振动 (FLOAT) === */
    dx FLOAT,                  -- [X轴位移] (μm)
    dy FLOAT,                  -- [Y轴位移] (μm)
    dz FLOAT,                  -- [Z轴位移] (μm)
    vx FLOAT,                  -- [X轴速度] (mm/s)
    vy FLOAT,                  -- [Y轴速度] (mm/s)
    vz FLOAT,                  -- [Z轴速度] (mm/s)

) TAGS (
    /* === 标签 (Tags)：用于设备管理与筛选 === */
    device_id BINARY(50),      -- [设备唯一编号] 例如: SN12345678
    ennergy_type BINARY(20)   -- [设备类型] 例如: ElecMeter, WaterMeter, GasMeter
);