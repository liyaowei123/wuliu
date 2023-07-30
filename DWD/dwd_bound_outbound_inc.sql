drop table if exists dwd_bound_outbound_inc;
create external table dwd_bound_outbound_inc(
                                                `id` bigint COMMENT '中转记录ID',
                                                `order_id` bigint COMMENT '订单ID',
                                                `org_id` bigint COMMENT '机构ID',
                                                `outbound_time` string COMMENT '出库时间',
                                                `outbound_emp_id` bigint COMMENT '出库人员'
) comment '中转域出库事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_bound_outbound_inc'
    tblproperties('orc.compress' = 'snappy');


-------------------------首日数据加载----------------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_outbound_inc
    partition (dt)
select after.id,
       after.order_id,
       after.org_id,
       date_format(from_utc_timestamp(
                           cast(after.outbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') outbound_time,
       after.outbound_emp_id,
       date_format(from_utc_timestamp(
                           cast(after.outbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd')          dt
from ods_order_org_bound_inc
where dt = '2023-01-10'
  and after.outbound_time is not null;

----------------------------每日数据加载-----------------------------------
insert overwrite table dwd_bound_outbound_inc
    partition (dt = '2023-01-11')
select after.id,
       after.order_id,
       after.org_id,
       date_format(from_utc_timestamp(
                           cast(after.outbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') outbound_time,
       after.outbound_emp_id
from ods_order_org_bound_inc
where dt = '2023-01-11'
  and op = 'u'
  and before.outbound_time is null
  and after.outbound_time is not null
  and after.is_deleted = '0';


