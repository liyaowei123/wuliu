drop table if exists ods_truck_model_full;
create external table ods_truck_model_full(
                                              id  bigint COMMENT '型号ID',
                                              model_name  string COMMENT '型号名称',
                                              model_type  string COMMENT '型号类型',
                                              model_no  string COMMENT '型号编码',
                                              brand  string COMMENT '品牌',
                                              truck_weight  decimal(16,2) COMMENT '整车重量（吨）',
                                              load_weight  decimal(16,2) COMMENT '额定载重（吨）',
                                              total_weight  decimal(16,2) COMMENT '总质量（吨）',
                                              eev  string COMMENT '排放标准',
                                              boxcar_len  decimal(16,2) COMMENT '货箱长（m）',
                                              boxcar_wd  decimal(16,2) COMMENT '货箱宽（m）',
                                              boxcar_hg  decimal(16,2) COMMENT '货箱高（m）',
                                              max_speed  bigint COMMENT '最高时速（千米/时）',
                                              oil_vol  bigint COMMENT '油箱容积（升）',
                                              create_time  string COMMENT '创建时间',
                                              update_time  string COMMENT '更新时间',
                                              is_deleted  string COMMENT '删除标记（0:不可用 1:可用）'
) comment '卡车型号表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_truck_model_full';
