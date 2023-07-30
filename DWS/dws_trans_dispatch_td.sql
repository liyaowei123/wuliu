drop table if exists dws_trans_dispatch_td;
create external table dws_trans_dispatch_td(
                                               `order_count` bigint comment '发单数',
                                               `order_amount` decimal(16,2) comment '发单金额'
) comment '物流域发单历史至今汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trans_dispatch_td'
    tblproperties('orc.compress'='snappy');

-----------------首日数据加载------------------
insert overwrite table dws_trans_dispatch_td
    partition (dt = '2023-01-20')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d;


----------------每日数据加载-------------------
insert overwrite table dws_trans_dispatch_td
    partition (dt = '2023-01-11')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from (select order_count,
             order_amount
      from dws_trans_dispatch_td
      where dt = date_add('2023-01-20', -1)
      union
      select order_count,
             order_amount
      from dws_trans_dispatch_1d
      where dt = '2023-01-20') all_data;
