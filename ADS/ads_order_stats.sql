drop table if exists ads_order_stats;
create external table ads_order_stats(
                                         `dt` string COMMENT '统计日期',
                                         `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
                                         `order_count` bigint COMMENT '下单数',
                                         `order_amount` decimal(16,2) COMMENT '下单金额'
) comment '运单综合统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_order_stats';

--数据加载--
insert overwrite table ads_order_stats
select dt,
       recent_days,
       order_count,
       order_amount
from ads_order_stats
union
select '2023-01-20'      dt,
       1                 recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d
where dt = '2023-01-20'
union
select '2023-01-20'      dt,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_nd
where dt = '2023-01-20'
group by recent_days;
