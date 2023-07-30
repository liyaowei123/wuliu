drop table if exists ods_line_base_info_full;
create external table ods_line_base_info_full(
                                                 id  bigint COMMENT '线路ID',
                                                 name  string COMMENT '线路名称',
                                                 line_no  string COMMENT '线路编号',
                                                 line_level  string COMMENT '线路级别',
                                                 org_id  bigint COMMENT '所属机构',
                                                 transport_line_type_id  string COMMENT '线路类型',
                                                 start_org_id  bigint COMMENT '起始机构ID',
                                                 start_org_name  string COMMENT '起始机构名称',
                                                 end_org_id  bigint COMMENT '目标机构ID',
                                                 end_org_name  string COMMENT '目标机构名称',
                                                 pair_line_id  bigint COMMENT '配对线路ID',
                                                 distance  decimal(10,2) COMMENT '预估里程',
                                                 cost  decimal(10,2) COMMENT '实际里程',
                                                 estimated_time  bigint COMMENT '预计时间（分钟）',
                                                 status  string COMMENT '状态 0：禁用 1：正常',
                                                 create_time  string COMMENT '创建时间',
                                                 update_time  string COMMENT '更新时间',
                                                 is_deleted  string COMMENT '删除标记（0:不可用 1:可用）'
) comment '运输线路表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_line_base_info_full';
