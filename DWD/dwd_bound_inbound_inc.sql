drop table if exists dwd_bound_inbound_inc;
create external table dwd_bound_inbound_inc(
                                               `id` bigint COMMENT '中转记录ID',
                                               `order_id` bigint COMMENT '运单ID',
                                               `org_id` bigint COMMENT '机构ID',
                                               `inbound_time` string COMMENT '入库时间',
                                               `inbound_emp_id` bigint COMMENT '入库人员'
) comment '中转域入库事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_bound_inbound_inc'
    tblproperties('orc.compress' = 'snappy');



------------------首日数据加载-------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_inbound_inc
    partition (dt)
select after.id,
       after.order_id,
       after.org_id,
       date_format(from_utc_timestamp(
                           cast(after.inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') inbound_time,
       after.inbound_emp_id,
       date_format(from_utc_timestamp(
                           cast(after.inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd')          dt
from ods_order_org_bound_inc
where dt = '2023-01-20';

-------------每日数据加载-------------
insert overwrite table dwd_bound_inbound_inc
    partition (dt = '2023-01-21')
select after.id,
       after.order_id,
       after.org_id,
       date_format(from_utc_timestamp(
                           cast(after.inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') inbound_time,
       after.inbound_emp_id
from ods_order_org_bound_inc
where dt = '2023-01-21'
  and op = 'c';

