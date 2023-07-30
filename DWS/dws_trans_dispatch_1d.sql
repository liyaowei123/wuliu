drop table if exists dws_trans_dispatch_1d;
create external table dws_trans_dispatch_1d(
                                               `order_count` bigint comment '发单总数',
                                               `order_amount` decimal(16,2) comment '发单总金额'
) comment '物流域发单 1 日汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trans_dispatch_1d/'
    tblproperties('orc.compress'='snappy');


-------------------------首日数据加载-----------------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_dispatch_1d
    partition (dt)
select count(order_id)      order_count,
       sum(distinct_amount) order_amount,
       dt
from (select order_id,
             dt,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail_inc
      group by order_id,
               dt) distinct_info
group by dt;


-------------------------每日数据加载---------------------------
insert overwrite table dws_trans_dispatch_1d
    partition (dt = '2023-01-20')
select count(order_id)      order_count,
       sum(distinct_amount) order_amount
from (select order_id,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail_inc
      where dt = '2023-01-20'
      group by order_id) distinct_info;
