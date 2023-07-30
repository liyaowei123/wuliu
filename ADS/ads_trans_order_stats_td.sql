drop table if exists ads_trans_order_stats_td;
create external table ads_trans_order_stats_td(
                                                  `dt` string COMMENT '统计日期',
                                                  `bounding_order_count` bigint COMMENT '运输中运单总数',
                                                  `bounding_order_amount` decimal(16,2) COMMENT '运输中运单金额'
) comment '历史至今运单统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_trans_order_stats_td';


--数据加载--
insert overwrite table ads_trans_order_stats_td
select dt,
       bounding_order_count,
       bounding_order_amount
from ads_trans_order_stats_td
union
select dt,
       sum(order_count)  bounding_order_count,
       sum(order_amount) bounding_order_amount
from (select dt,
             order_count,
             order_amount
      from dws_trans_dispatch_td
      where dt = '2023-01-20'
      union
      select dt,
             order_count * (-1),
             order_amount * (-1)
      from dws_trans_bound_finish_td
      where dt = '2023-01-20') new
group by dt;
