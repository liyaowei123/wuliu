drop table if exists ods_base_complex_full;
create external table ods_base_complex_full(
    id  bigint comment '小区ID',
    complex_name  string comment '小区名称',
    province_id  bigint comment '省份ID',
    city_id  bigint comment '城市ID',
    district_id  bigint comment '区（县）ID',
    district_name  string comment '区（县）名称',
    create_time  string comment '创建时间',
    update_time  string comment '更新时间',
    is_deleted  string comment '是否删除'
) comment '小区表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_base_complex_full';