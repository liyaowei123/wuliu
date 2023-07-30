use tms;

drop table ods_transport_task_inc;

drop table if exists ods_transport_task_inc;
create external table ods_transport_task_inc(
                                                `op` string comment '操作类型',
                                                `after` struct<`id`:bigint,`shift_id`:bigint,`line_id`:bigint,`start_org_id`:bigint,`start_org_name`:string,`end_org_id`:bigint,`end_org_name`:string,`status`:string,`order_num`:bigint,`driver1_emp_id`:bigint,`driver1_name`:string,`driver2_emp_id`:bigint,`driver2_name`:string,`truck_id`:bigint,`truck_no`:string,`actual_start_time`:string,`actual_end_time`:string,`actual_distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '插入或修改后的数据',
                                                `before` struct<`id`:bigint,`shift_id`:bigint,`line_id`:bigint,`start_org_id`:bigint,`start_org_name`:string,`end_org_id`:bigint,`end_org_name`:string,`status`:string,`order_num`:bigint,`driver1_emp_id`:bigint,`driver1_name`:string,`driver2_emp_id`:bigint,`driver2_name`:string,`truck_id`:bigint,`truck_no`:string,`actual_start_time`:string,`actual_end_time`:string,`actual_distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '修改前的数据',
                                                `ts` bigint comment '时间戳'
) comment '运输任务表'
    partitioned by (`dt` string comment '统计日期')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/tms/ods/ods_transport_task_inc';


load data inpath '/origin_data/tms/transport_task_inc/2023-01-20' overwrite into table ods_transport_task_inc partition (dt = '2023-01-20');

select * from ods_transport_task_inc;

