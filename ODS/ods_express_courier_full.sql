drop table if exists ods_express_courier_full;
create external table ods_express_courier_full(
                                                  id  bigint COMMENT '快递员ID',
                                                  emp_id  bigint COMMENT '员工ID',
                                                  org_id  bigint COMMENT '所属机构ID',
                                                  working_phone  string COMMENT '工作电话',
                                                  express_type  string COMMENT '快递员类型（收货；发货）',
                                                  create_time  string COMMENT '创建时间',
                                                  update_time  string COMMENT '更新时间',
                                                  is_deleted  string COMMENT '删除标记（0:不可用 1:可用）'
) comment '快递员信息表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_express_courier_full';
