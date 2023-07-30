drop table if exists ods_express_courier_complex_full;
create external table ods_express_courier_complex_full(
                                                          id  bigint COMMENT '主键ID',
                                                          courier_emp_id  bigint COMMENT '快递员ID',
                                                          complex_id  bigint COMMENT '小区ID',
                                                          create_time  string COMMENT '创建时间',
                                                          update_time  string COMMENT '更新时间',
                                                          is_deleted  string COMMENT '删除标记（0:不可用 1:可用）'
) comment '快递员小区关联表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_express_courier_complex_full';
