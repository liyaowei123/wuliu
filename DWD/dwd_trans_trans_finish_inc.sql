drop table if exists dwd_trans_trans_finish_inc;
create external table dwd_trans_trans_finish_inc(
                                                    `id` bigint comment '运输任务ID',
                                                    `shift_id` bigint COMMENT '车次ID',
                                                    `line_id` bigint COMMENT '路线ID',
                                                    `start_org_id` bigint COMMENT '起始机构ID',
                                                    `start_org_name` string COMMENT '起始机构名称',
                                                    `end_org_id` bigint COMMENT '目的机构ID',
                                                    `end_org_name` string COMMENT '目的机构名称',
                                                    `order_num` bigint COMMENT '运单个数',
                                                    `driver1_emp_id` bigint COMMENT '司机1ID',
                                                    `driver1_name` string COMMENT '司机1名称',
                                                    `driver2_emp_id` bigint COMMENT '司机2ID',
                                                    `driver2_name` string COMMENT '司机2名称',
                                                    `truck_id` bigint COMMENT '卡车ID',
                                                    `truck_no` string COMMENT '卡车号牌',
                                                    `actual_start_time` string COMMENT '实际启动时间',
                                                    `actual_end_time` string COMMENT '实际到达时间',
                                                    `estimate_end_time` string COMMENT '预估到达时间',
                                                    `actual_distance` decimal(16,2) COMMENT '实际行驶距离',
                                                    `finish_dur_sec` bigint COMMENT '运输完成历经时长：秒',
                                                    `ts` bigint COMMENT '时间戳'
) comment '物流域运输事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_trans_finish_inc'
    tblproperties('orc.compress' = 'snappy');

--------------------------首日数据加载----------------------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_trans_finish_inc
    partition (dt)
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       date_format(from_utc_timestamp(cast(actual_end_time as bigint), 'Asia/Shanghai'), 'yyyy-MM-dd HH:mm:ss') actual_end_time,
       date_format(from_utc_timestamp(actual_end_time / 1000 + estimated_time * 60, 'Asia/Shanghai'), 'yyyy-MM-ss HH:mm:ss') estimate_end_time,
       actual_distance,
       finish_dur_sec,
       ts,
       dt
from (select after.id,
             after.shift_id,
             after.line_id,
             after.start_org_id,
             after.start_org_name,
             after.end_org_id,
             after.end_org_name,
             after.order_num,
             after.driver1_emp_id,
             concat(substr(after.driver1_name, 1, 1), '*')                                            driver1_name,
             after.driver2_emp_id,
             concat(substr(after.driver2_name, 1, 1), '*')                                            driver2_name,
             after.truck_id,
             md5(after.truck_no)                                                                      truck_no,
             date_format(from_utc_timestamp(
                                 cast(after.actual_start_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_start_time,
             after.actual_end_time,

             after.actual_distance,
             (cast(after.actual_end_time as bigint) - cast(after.actual_start_time as bigint)) / 1000 finish_dur_sec,
             ts,
             date_format(from_utc_timestamp(
                                 cast(after.actual_end_time as bigint), 'UTC'),
                         'yyyy-MM-dd')                                                                dt
      from ods_transport_task_inc
      where dt = '2023-01-20'
        and after.is_deleted = '0'
        and after.actual_end_time is not null) info
         left join
     (select id,
             estimated_time
      from ods_line_base_info_full
      where dt = '2023-01-20') dim_tb
     on info.shift_id = dim_tb.id;



-----------------------------每日数据加载-------------------------------
insert overwrite table dwd_trans_trans_finish_inc
    partition (dt = '2023-01-21')
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       actual_end_time,
       from_unixtime( (to_unix_timestamp(actual_start_time) + estimated_time*60)) estimate_end_time,
       actual_distance,
       finish_dur_sec,
       ts
from (select after.id,
             after.shift_id,
             after.line_id,
             after.start_org_id,
             after.start_org_name,
             after.end_org_id,
             after.end_org_name,
             after.order_num,
             after.driver1_emp_id,
             concat(substr(after.driver1_name, 1, 1), '*')                                            driver1_name,
             after.driver2_emp_id,
             concat(substr(after.driver2_name, 1, 1), '*')                                            driver2_name,
             after.truck_id,
             md5(after.truck_no)                                                                      truck_no,
             date_format(from_utc_timestamp(
                                 cast(after.actual_start_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_start_time,
             date_format(from_utc_timestamp(
                                 cast(after.actual_end_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                                       actual_end_time,
             after.actual_distance,
             (cast(after.actual_end_time as bigint) - cast(after.actual_start_time as bigint)) / 1000 finish_dur_sec,
             ts                                                                                       ts
      from ods_transport_task_inc
      where dt = '2023-01-21'
        and op = 'u'
        and before.actual_end_time is null
        and after.actual_end_time is not null
        and after.is_deleted = '0') info
         left join
     (select id,
             estimated_time
      from dim_shift_full
      where dt = '2023-01-20') dim_tb
     on info.shift_id = dim_tb.id;
