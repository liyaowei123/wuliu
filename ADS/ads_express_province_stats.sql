drop table if exists ads_express_province_stats;
create external table ads_express_province_stats(
  `dt` string COMMENT '统计日期',
  `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `province_id` bigint COMMENT '省份ID',
  `province_name` string COMMENT '省份名称',
  `receive_order_count` bigint COMMENT '揽收次数',
  `receive_order_amount` decimal(16,2) COMMENT '揽收金额',
  `deliver_suc_count` bigint COMMENT '派送成功次数',
  `sort_count` bigint COMMENT '分拣次数'
) comment '各省份快递统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_province_stats';

--数据加载--
insert overwrite table ads_express_province_stats
select dt,
       recent_days,
       province_id,
       province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from ads_express_province_stats
union
select nvl(nvl(province_deliver_1d.dt, province_sort_1d.dt), province_receive_1d.dt) dt,
       nvl(nvl(province_deliver_1d.recent_days, province_sort_1d.recent_days),
           province_receive_1d.recent_days)                                          recent_days,
       nvl(nvl(province_deliver_1d.province_id, province_sort_1d.province_id),
           province_receive_1d.province_id)                                          province_id,
       nvl(nvl(province_deliver_1d.province_name, province_sort_1d.province_name),
           province_receive_1d.province_name)                                        province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '2023-01-20'     dt,
             1                recent_days,
             province_id,
             province_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '2023-01-20'
      group by province_id,
               province_name) province_deliver_1d
         full outer join
     (select '2023-01-20'    dt,
             1               recent_days,
             province_id,
             province_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '2023-01-20'
      group by province_id,
               province_name) province_sort_1d
     on province_deliver_1d.dt = province_sort_1d.dt
         and province_deliver_1d.recent_days = province_sort_1d.recent_days
         and province_deliver_1d.province_id = province_sort_1d.province_id
         and province_deliver_1d.province_name = province_sort_1d.province_name
         full outer join
     (select '2023-01-20'      dt,
             1                 recent_days,
             province_id,
             province_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '2023-01-20'
      group by province_id,
               province_name) province_receive_1d
     on province_deliver_1d.dt = province_receive_1d.dt
         and province_deliver_1d.recent_days = province_receive_1d.recent_days
         and province_deliver_1d.province_id = province_receive_1d.province_id
         and province_deliver_1d.province_name = province_receive_1d.province_name
union
select nvl(nvl(province_deliver_nd.dt, province_sort_nd.dt), province_receive_nd.dt) dt,
       nvl(nvl(province_deliver_nd.recent_days, province_sort_nd.recent_days),
           province_receive_nd.recent_days)                                          recent_days,
       nvl(nvl(province_deliver_nd.province_id, province_sort_nd.province_id),
           province_receive_nd.province_id)                                          province_id,
       nvl(nvl(province_deliver_nd.province_name, province_sort_nd.province_name),
           province_receive_nd.province_name)                                        province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '2023-01-20'     dt,
             recent_days,
             province_id,
             province_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '2023-01-20'
      group by recent_days,
               province_id,
               province_name) province_deliver_nd
         full outer join
     (select '2023-01-20'    dt,
             recent_days,
             province_id,
             province_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '2023-01-20'
      group by recent_days,
               province_id,
               province_name) province_sort_nd
     on province_deliver_nd.dt = province_sort_nd.dt
         and province_deliver_nd.recent_days = province_sort_nd.recent_days
         and province_deliver_nd.province_id = province_sort_nd.province_id
         and province_deliver_nd.province_name = province_sort_nd.province_name
         full outer join
     (select '2023-01-20'      dt,
             recent_days,
             province_id,
             province_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '2023-01-20'
      group by recent_days,
               province_id,
               province_name) province_receive_nd
     on province_deliver_nd.dt = province_receive_nd.dt
         and province_deliver_nd.recent_days = province_receive_nd.recent_days
         and province_deliver_nd.province_id = province_receive_nd.province_id
         and province_deliver_nd.province_name = province_receive_nd.province_name;
