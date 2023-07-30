drop table if exists ods_truck_info_full;
create external table ods_truck_info_full(
                                             id  bigint COMMENT '卡车ID',
                                             team_id  bigint COMMENT '所属车队ID',
                                             truck_no  string COMMENT '车牌号码',
                                             truck_model_id  string COMMENT '型号',
                                             device_gps_id  string COMMENT 'GPS设备ID',
                                             engine_no  string COMMENT '发动机编码',
                                             license_registration_date  string COMMENT '注册时间',
                                             license_last_check_date  string COMMENT '最后年检日期',
                                             license_expire_date  string COMMENT '失效日期',
                                             picture_url  string COMMENT '图片链接',
                                             is_enabled  tinyint COMMENT '状态 0：禁用 1：正常',
                                             create_time  string COMMENT '创建时间',
                                             update_time  string COMMENT '更新时间',
                                             is_deleted  string COMMENT '删除标记（0:不可用 1:可用）'
) comment '卡车信息表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_truck_info_full';
