drop table if exists ads_trans_order_stats;
create external table ads_trans_order_stats(
  `dt` string COMMENT '统计日期',
  `recent_days` tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `receive_order_count` bigint COMMENT '接单总数',
  `receive_order_amount` decimal(16,2) COMMENT '接单金额',
  `dispatch_order_count` bigint COMMENT '发单总数',
  `dispatch_order_amount` decimal(16,2) COMMENT '发单金额'
) comment '运单相关统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_trans_order_stats';


-----------数据加载-----------
insert overwrite table ads_trans_order_stats
select dt,
       recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from ads_trans_order_stats
union
select '2023-01-20'                                         dt,
       nvl(receive_1d.recent_days, dispatch_1d.recent_days) recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from (select 1                 recent_days,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '2023-01-20') receive_1d
         full outer join
     (select 1            recent_days,
             order_count  dispatch_order_count,
             order_amount dispatch_order_amount
      from dws_trans_dispatch_1d
      where dt = '2023-01-20') dispatch_1d
     on receive_1d.recent_days = dispatch_1d.recent_days
union
select '2023-01-20'                                         dt,
       nvl(receive_nd.recent_days, dispatch_nd.recent_days) recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from (select recent_days,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '2023-01-20'
      group by recent_days) receive_nd
         full outer join
     (select recent_days,
             order_count  dispatch_order_count,
             order_amount dispatch_order_amount
      from dws_trans_dispatch_nd
      where dt = '2023-01-20') dispatch_nd
     on receive_nd.recent_days = dispatch_nd.recent_days;
