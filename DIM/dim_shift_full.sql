drop table if exists dim_shift_full;
create external table dim_shift_full(
                                        `id` bigint COMMENT '班次ID',
                                        `line_id` bigint COMMENT '线路ID',
                                        `line_name` string COMMENT '线路名称',
                                        `line_no` string COMMENT '线路编号',
                                        `line_level` string COMMENT '线路级别',
                                        `org_id` bigint COMMENT '所属机构',
                                        `transport_line_type_id` string COMMENT '线路类型ID',
                                        `transport_line_type_name` string COMMENT '线路类型名称',
                                        `start_org_id` bigint COMMENT '起始机构ID',
                                        `start_org_name` string COMMENT '起始机构名称',
                                        `end_org_id` bigint COMMENT '目标机构ID',
                                        `end_org_name` string COMMENT '目标机构名称',
                                        `pair_line_id` bigint COMMENT '配对线路ID',
                                        `distance` decimal(10,2) COMMENT '直线距离',
                                        `cost` decimal(10,2) COMMENT '公路里程',
                                        `estimated_time` bigint COMMENT '预计时间（分钟）',
                                        `start_time` string COMMENT '班次开始时间',
                                        `driver1_emp_id` bigint COMMENT '第一司机',
                                        `driver2_emp_id` bigint COMMENT '第二司机',
                                        `truck_id` bigint COMMENT '卡车ID',
                                        `pair_shift_id` bigint COMMENT '配对班次(同一辆车一去一回的另一班次)'
) comment '班次维度表'
    partitioned by (`dt` string comment '统计周期')
    stored as orc
    location '/warehouse/tms/DIM/dim_shift_full'
    tblproperties('orc.compress'='snappy');


insert overwrite table tms.dim_shift_full
    partition (dt = '2023-01-20')
select shift_info.id,
       line_id,
       line_info.name line_name,
       line_no,
       line_level,
       org_id,
       transport_line_type_id,
       dic_info.name  transport_line_type_name,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       pair_line_id,
       distance,
       cost,
       estimated_time,
       start_time,
       driver1_emp_id,
       driver2_emp_id,
       truck_id,
       pair_shift_id
from (select id,
             line_id,
             start_time,
             driver1_emp_id,
             driver2_emp_id,
             truck_id,
             pair_shift_id
      from ods_line_base_shift_full
      where dt = '2023-01-20'
        and is_deleted = '0') shift_info
         join
     (select id,
             name,
             line_no,
             line_level,
             org_id,
             transport_line_type_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             pair_line_id,
             distance,
             cost,
             estimated_time
      from ods_line_base_info_full
      where dt = '2023-01-20'
        and is_deleted = '0') line_info
     on shift_info.line_id = line_info.id
         join (
    select id,
           name
    from ods_base_dic_full
    where dt = '2023-01-20'
      and is_deleted = '0'
) dic_info on line_info.transport_line_type_id = dic_info.id;
