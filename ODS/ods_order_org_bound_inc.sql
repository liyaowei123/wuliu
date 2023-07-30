use tms;
drop table if exists ods_order_org_bound_inc;
create external table ods_order_org_bound_inc(
                                                 `op` string comment '操作类型',
                                                 `after` struct<`id`:bigint,`order_id`:bigint,`org_id`:bigint,`status`:string,`inbound_time`:string,`inbound_emp_id`:bigint,`sort_time`:string,`sorter_emp_id`:bigint,`outbound_time`:string,`outbound_emp_id`:bigint,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '插入或修改后的数据',
                                                 `before` struct<`id`:bigint,`order_id`:bigint,`org_id`:bigint,`status`:string,`inbound_time`:string,`inbound_emp_id`:bigint,`sort_time`:string,`sorter_emp_id`:bigint,`outbound_time`:string,`outbound_emp_id`:bigint,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '修改之前的数据',
                                                 `ts` bigint comment '时间戳'
) comment '运单机构中转表'
    partitioned by (`dt` string comment '统计日期')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/tms/ods/ods_order_org_bound_inc';


load data inpath '/origin_data/tms/order_org_bound_inc/2023-01-10' overwrite into table ods_order_org_bound_inc partition (dt = '2023-01-10');

select * from ods_order_org_bound_inc;

show tables;