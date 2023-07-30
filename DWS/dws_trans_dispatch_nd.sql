drop table if exists dws_trans_dispatch_nd;
create external table dws_trans_dispatch_nd(
                                               `recent_days` tinyint comment '最近天数',
                                               `order_count` bigint comment '发单总数',
                                               `order_amount` decimal(16,2) comment '发单总金额'
) comment '物流域发单 1 日汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trans_dispatch_nd/'
    tblproperties('orc.compress'='snappy');

-------------------数据加载------------------------
insert overwrite table dws_trans_dispatch_nd
    partition (dt = '2023-01-10')
select recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-01-20', -recent_days + 1)
group by recent_days;
