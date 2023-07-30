drop table if exists ads_express_stats;
create external table ads_express_stats(
                                           `dt` string COMMENT '统计日期',
                                           `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
                                           `deliver_suc_count` bigint COMMENT '派送成功次数（订单数）',
                                           `sort_count` bigint COMMENT '分拣次数'
) comment '快递综合统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_stats';

--数据加载--
insert overwrite table ads_express_stats
select dt,
       recent_days,
       deliver_suc_count,
       sort_count
from ads_express_stats
union
select nvl(deliver_1d.dt, sort_1d.dt)                   dt,
       nvl(deliver_1d.recent_days, sort_1d.recent_days) recent_days,
       deliver_suc_count,
       sort_count
from (select '2023-01-20'     dt,
             1                recent_days,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '2023-01-20') deliver_1d
         full outer join
     (select '2023-01-20'    dt,
             1               recent_days,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '2023-01-20') sort_1d
     on deliver_1d.dt = sort_1d.dt
         and deliver_1d.recent_days = sort_1d.recent_days
union
select nvl(deliver_nd.dt, sort_nd.dt)                   dt,
       nvl(deliver_nd.recent_days, sort_nd.recent_days) recent_days,
       deliver_suc_count,
       sort_count
from (select '2023-01-20'     dt,
             recent_days,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '2023-01-20'
      group by recent_days) deliver_nd
         full outer join
     (select '2023-01-20'    dt,
             recent_days,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '2023-01-20'
      group by recent_days) sort_nd
     on deliver_nd.dt = sort_nd.dt
         and deliver_nd.recent_days = sort_nd.recent_days;
