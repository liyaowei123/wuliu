drop table if exists dws_trade_org_cargo_type_order_nd;
create external table dws_trade_org_cargo_type_order_nd(
                                                           `org_id` bigint comment '机构ID',
                                                           `org_name` string comment '转运站名称',
                                                           `city_id` bigint comment '城市ID',
                                                           `city_name` string comment '城市名称',
                                                           `cargo_type` string comment '货物类型',
                                                           `cargo_type_name` string comment '货物类型名称',
                                                           `recent_days` tinyint comment '最近天数',
                                                           `order_count` bigint comment '下单数',
                                                           `order_amount` decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单 n 日汇总表'
    partitioned by(`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trade_org_cargo_type_order_nd'
    tblproperties('orc.compress' = 'snappy');

-------------------------数据加载-----------------------------
insert overwrite table dws_trade_org_cargo_type_order_nd
    partition (dt = '2023-01-20')
select org_id,
       org_name,
       city_id,
       city_name,
       cargo_type,
       cargo_type_name,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-01-20', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         cargo_type,
         cargo_type_name,
         recent_days;