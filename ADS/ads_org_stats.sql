drop table if exists ads_org_stats;
create external table ads_org_stats(
                                       `dt` string COMMENT '统计日期',
                                       `recent_days` tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
                                       `org_id` bigint COMMENT '机构ID',
                                       `org_name` string COMMENT '机构名称',
                                       `order_count` bigint COMMENT '下单数',
                                       `order_amount` decimal COMMENT '下单金额',
                                       `trans_finish_count` bigint COMMENT '完成运输次数',
                                       `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
                                       `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
                                       `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
                                       `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒'
) comment '机构分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_org_stats';

--数据加载--
insert overwrite table ads_org_stats
select dt,
       recent_days,
       org_id,
       org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from ads_org_stats
union
select nvl(org_order_1d.dt, org_trans_1d.dt)                   dt,
       nvl(org_order_1d.recent_days, org_trans_1d.recent_days) recent_days,
       nvl(org_order_1d.org_id, org_trans_1d.org_id)           org_id,
       nvl(org_order_1d.org_name, org_trans_1d.org_name)       org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '2023-01-20'      dt,
             1                 recent_days,
             org_id,
             org_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_1d
      where dt = '2023-01-20'
      group by org_id,
               org_name) org_order_1d
         full outer join
     (select '2023-01-20'                                         dt,
             org_id,
             org_name,
             1                                                    recent_days,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from dws_trans_org_truck_model_type_trans_finish_1d
      where dt = '2023-01-20'
      group by org_id,
               org_name
     ) org_trans_1d
     on org_order_1d.dt = org_trans_1d.dt
         and org_order_1d.recent_days = org_trans_1d.recent_days
         and org_order_1d.org_id = org_trans_1d.org_id
         and org_order_1d.org_name = org_trans_1d.org_name
union
select org_order_nd.dt,
       org_order_nd.recent_days,
       org_order_nd.org_id,
       org_order_nd.org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '2023-01-20'      dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_nd
      where dt = '2023-01-20'
      group by org_id,
               org_name,
               recent_days) org_order_nd
         join
     (select '2023-01-20'                                         dt,
             recent_days,
             org_id,
             org_name,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from dws_trans_shift_trans_finish_nd
      where dt = '2023-01-20'
      group by org_id,
               org_name,
               recent_days
     ) org_trans_nd
     on org_order_nd.dt = org_trans_nd.dt
         and org_order_nd.recent_days = org_trans_nd.recent_days
         and org_order_nd.org_id = org_trans_nd.org_id
         and org_order_nd.org_name = org_trans_nd.org_name;
