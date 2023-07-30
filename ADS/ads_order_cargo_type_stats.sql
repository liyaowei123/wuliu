drop table if exists ads_order_cargo_type_stats;
create external table ads_order_cargo_type_stats(
  `dt` string COMMENT '统计日期',
  `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `cargo_type` string COMMENT '货物类型',
  `cargo_type_name` string COMMENT '货物类型名称',
  `order_count` bigint COMMENT '下单数',
  `order_amount` decimal(16,2) COMMENT '下单金额'
) comment '各类型货物运单统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_order_cargo_type_stats';

--数据加载--
insert overwrite table ads_order_cargo_type_stats
select dt,
       recent_days,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount
from ads_order_cargo_type_stats
union
select '2023-01-10'      dt,
       1                 recent_days,
       cargo_type,
       cargo_type_name,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d
where dt = '2023-01-20'
group by cargo_type,
         cargo_type_name
union
select '2023-01-20'      dt,
       recent_days,
       cargo_type,
       cargo_type_name,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_nd
where dt = '2023-01-20'
group by cargo_type,
         cargo_type_name,
         recent_days;
