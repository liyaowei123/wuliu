drop table if exists ods_base_organ_full;
create external table ods_base_organ_full(
                                             id  bigint COMMENT '机构ID',
                                             org_name  string COMMENT '机构名称',
                                             org_level  bigint COMMENT '机构等级（1为转运中心，2为转运站）',
                                             region_id  bigint COMMENT '地区ID，1级机构为city ,2级机构为district',
                                             org_parent_id  bigint COMMENT '父级机构ID',
                                             points  string COMMENT '多边形经纬度坐标集合',
                                             create_time  string COMMENT '创建时间',
                                             update_time  string COMMENT '更新时间',
                                             is_deleted  string COMMENT '删除标记（0:不可用 1:可用）'
) comment '机构表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_base_organ_full';

