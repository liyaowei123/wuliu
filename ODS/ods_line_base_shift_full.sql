drop table if exists ods_line_base_shift_full;
create external table ods_line_base_shift_full(
                                                  id  bigint COMMENT '班次ID',
                                                  line_id  bigint COMMENT '线路ID',
                                                  start_time  string COMMENT '班次开始时间',
                                                  driver1_emp_id  bigint COMMENT '第一司机',
                                                  driver2_emp_id  bigint COMMENT '第二司机',
                                                  truck_id  bigint COMMENT '卡车',
                                                  pair_shift_id  bigint COMMENT '配对班次(同一辆车一去一回的另一班次)',
                                                  is_enabled  string COMMENT '状态 0：禁用 1：正常',
                                                  create_time  string COMMENT '创建时间',
                                                  update_time  string COMMENT '更新时间',
                                                  is_deleted  string COMMENT '删除标记（0:不可用 1:可用）'
) comment '班次表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_line_base_shift_full';
