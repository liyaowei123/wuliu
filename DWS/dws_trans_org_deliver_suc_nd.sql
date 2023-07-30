drop table if exists dws_trans_org_deliver_suc_nd;
create external table dws_trans_org_deliver_suc_nd(
                                                      `org_id` bigint comment '转运站ID',
                                                      `org_name` string comment '转运站名称',
                                                      `city_id` bigint comment '城市ID',
                                                      `city_name` string comment '城市名称',
                                                      `province_id` bigint comment '省份ID',
                                                      `province_name` string comment '省份名称',
                                                      `recent_days` tinyint comment '最近天数',
                                                      `order_count` bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功 n 日汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trans_org_deliver_suc_nd/'
    tblproperties('orc.compress'='snappy');

-----------------------数据加载--------------------------
insert overwrite table dws_trans_org_deliver_suc_nd
    partition (dt = '2023-01-10')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count) order_count
from dws_trans_org_deliver_suc_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-01-20', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
