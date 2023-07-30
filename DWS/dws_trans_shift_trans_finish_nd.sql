drop table if exists dws_trans_shift_trans_finish_nd;
create external table dws_trans_shift_trans_finish_nd(
                                                         `shift_id` bigint comment '班次ID',
                                                         `city_id` bigint comment '城市ID',
                                                         `city_name` string comment '城市名称',
                                                         `org_id` bigint comment '机构ID',
                                                         `org_name` string comment '机构名称',
                                                         `line_id` bigint comment '线路ID',
                                                         `line_name` string comment '线路名称',
                                                         `driver1_emp_id` bigint comment '第一司机员工ID',
                                                         `driver1_name` string comment '第一司机姓名',
                                                         `driver2_emp_id` bigint comment '第二司机员工ID',
                                                         `driver2_name` string comment '第二司机姓名',
                                                         `truck_model_type` string comment '卡车类别编码',
                                                         `truck_model_type_name` string comment '卡车类别名称',
                                                         `recent_days` tinyint comment '最近天数',
                                                         `trans_finish_count` bigint comment '转运完成次数',
                                                         `trans_finish_distance` decimal(16,2) comment '转运完成里程',
                                                         `trans_finish_dur_sec` bigint comment '转运完成时长，单位：秒',
                                                         `trans_finish_order_count` bigint comment '转运完成运单数',
                                                         `trans_finish_delay_count` bigint comment '逾期次数'
) comment '物流域班次粒度转运完成最近 n 日汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trans_shift_trans_finish_nd/'
    tblproperties('orc.compress'='snappy');

----------------------数据加载------------------------
insert overwrite table dws_trans_shift_trans_finish_nd
    partition (dt = '2023-01-10')
select shift_id,
       if(org_level = 1, first.region_id, city.id)     city_id,
       if(org_level = 1, first.region_name, city.name) city_name,
       org_id,
       org_name,
       line_id,
       line_name,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_model_type,
       truck_model_type_name,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count,
       trans_finish_delay_count
from (select recent_days,
             shift_id,
             line_id,
             truck_id,
             start_org_id                                       org_id,
             start_org_name                                     org_name,
             driver1_emp_id,
             driver1_name,
             driver2_emp_id,
             driver2_name,
             count(id)                                          trans_finish_count,
             sum(actual_distance)                               trans_finish_distance,
             sum(finish_dur_sec)                                trans_finish_dur_sec,
             sum(order_num)                                     trans_finish_order_count,
             sum(if(actual_end_time > estimate_end_time, 1, 0)) trans_finish_delay_count
      from dwd_trans_trans_finish_inc lateral view
          explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2023-01-20', -recent_days + 1)
      group by recent_days,
               shift_id,
               line_id,
               start_org_id,
               start_org_name,
               driver1_emp_id,
               driver1_name,
               driver2_emp_id,
               driver2_name,
               truck_id) aggregated
         left join
     (select id,
             org_level,
             region_id,
             region_name
      from dim_organ_full
      where dt = '2023-01-20'
     ) first
     on aggregated.org_id = first.id
         left join
     (select id,
             parent_id
      from dim_region_full
      where dt = '2023-01-20'
     ) parent
     on first.region_id = parent.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '2023-01-20'
     ) city
     on parent.parent_id = city.id
         left join
     (select id,
             line_name
      from dim_shift_full
      where dt = '2023-01-20') for_line_name
     on shift_id = for_line_name.id
         left join (
    select id,
           truck_model_type,
           truck_model_type_name
    from dim_truck_full
    where dt = '2023-01-20'
) truck_info on truck_id = truck_info.id;
