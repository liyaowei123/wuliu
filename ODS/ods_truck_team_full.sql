drop table if exists ods_truck_team_full;
create external table ods_truck_team_full(
                                             id  bigint COMMENT '车队ID',
                                             name  string COMMENT '车队名称',
                                             team_no  string COMMENT '车队编号',
                                             org_id  bigint COMMENT '所属机构',
                                             manager_emp_id  bigint COMMENT '负责人',
                                             create_time  string COMMENT '创建时间',
                                             update_time  string COMMENT '更新时间',
                                             is_deleted  string COMMENT '删除标记（0:不可用 1:可用）'
) comment '车队信息表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_truck_team_full';
