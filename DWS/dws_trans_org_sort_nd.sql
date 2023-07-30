drop table if exists dws_trans_org_sort_nd;
create external table dws_trans_org_sort_nd(
                                               `org_id` bigint comment '机构ID',
                                               `org_name` string comment '机构名称',
                                               `city_id` bigint comment '城市ID',
                                               `city_name` string comment '城市名称',
                                               `province_id` bigint comment '省份ID',
                                               `province_name` string comment '省份名称',
                                               `recent_days` tinyint comment '最近天数',
                                               `sort_count` bigint comment '分拣次数'
) comment '物流域机构粒度分拣 n 日汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trans_org_sort_nd/'
    tblproperties('orc.compress'='snappy');

-------------------数据加载-----------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_sort_nd
    partition (dt = '2023-01-10')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(sort_count) sort_count
from dws_trans_org_sort_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2023-01-20', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
