drop table if exists dws_trans_org_receive_1d;
create external table dws_trans_org_receive_1d(
                                                  `org_id` bigint comment '转运站ID',
                                                  `org_name` string comment '转运站名称',
                                                  `city_id` bigint comment '城市ID',
                                                  `city_name` string comment '城市名称',
                                                  `province_id` bigint comment '省份ID',
                                                  `province_name` string comment '省份名称',
                                                  `order_count` bigint comment '揽收次数',
                                                  `order_amount` decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收 1 日汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trans_org_receive_1d/'
    tblproperties ('orc.compress'='snappy');


---------------------------首日数据加载-------------------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_receive_1d
    partition (dt)

-----------------------每日数据加载--------------------------
insert overwrite table dws_trans_org_receive_1d
    partition (dt = '2023-01-11')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       count(order_id)      order_count,
       sum(distinct_amount) order_amount
from (select order_id,
             org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             max(amount) distinct_amount
      from (select order_id,
                   amount,
                   sender_district_id
            from dwd_trans_receive_detail_inc
            where dt = '2023-01-20') detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '2023-01-20') organ
           on detail.sender_district_id = organ.region_id
               left join
           (select id,
                   parent_id
            from dim_region_full
            where dt = '2023-01-20') district
           on region_id = district.id
               left join
           (select id   city_id,
                   name city_name,
                   parent_id
            from dim_region_full
            where dt = '2023-01-20') city
           on district.parent_id = city_id
               left join
           (select id   province_id,
                   name province_name,
                   parent_id
            from dim_region_full
            where dt = '2023-01-20') province
           on city.parent_id = province_id
      group by order_id,
               org_id,
               org_name,
               city_id,
               city_name,
               province_id,
               province_name) distinct_tb
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name;
