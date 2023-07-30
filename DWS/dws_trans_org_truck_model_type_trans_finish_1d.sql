drop table if exists dws_trans_org_truck_model_type_trans_finish_1d;
create external table dws_trans_org_truck_model_type_trans_finish_1d(
                                                                        `org_id` bigint comment '机构ID',
                                                                        `org_name` string comment '机构名称',
                                                                        `truck_model_type` string comment '卡车类别编码',
                                                                        `truck_model_type_name` string comment '卡车类别名称',
                                                                        `trans_finish_count` bigint comment '运输完成次数',
                                                                        `trans_finish_distance` decimal(16,2) comment '运输完成里程',
                                                                        `trans_finish_dur_sec` bigint comment '运输完成时长，单位：秒'
) comment '物流域机构卡车类别粒度运输最近 1 日汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trans_org_truck_model_type_trans_finish_1d/'
    tblproperties('orc.compress'='snappy');

-------------------首日数据加载----------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt)
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    finish_dur_sec,
       dt
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec,
             dt
      from dwd_trans_trans_finish_inc) trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck_full
      where dt = '2023-01-20') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name,
         dt;


-----------------每日数据加载--------------------
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt = '2023-01-11')
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    finish_dur_sec
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec
      from dwd_trans_trans_finish_inc
      where dt = '2023-01-20') trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck_full
      where dt = '2023-01-20') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name;
