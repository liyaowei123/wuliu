drop table if exists dws_trans_org_deliver_suc_1d;
create external table dws_trans_org_deliver_suc_1d(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`order_count` bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功 1 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_org_deliver_suc_1d/'
	tblproperties('orc.compress'='snappy');


----------------------首日数据加载------------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_deliver_suc_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       city.name       city_name,
       province_id,
       province.name   province_name,
       count(order_id) order_count,
       dt
from (select order_id,
             receiver_district_id,
             dt
      from dwd_trans_deliver_suc_detail_inc
      group by order_id, receiver_district_id, dt) detail
         left join
     (select id        org_id,
             org_name,
             region_id district_id
      from dim_organ_full
      where dt = '2023-01-20') organ
     on detail.receiver_district_id = organ.district_id
         left join
     (select id,
             parent_id city_id
      from dim_region_full
      where dt = '2023-01-20') district
     on district_id = district.id
         left join
     (select id,
             name,
             parent_id province_id
      from dim_region_full
      where dt = '2023-01-20') city
     on city_id = city.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '2023-01-20') province
     on province_id = province.id
group by org_id,
         org_name,
         city_id,
         city.name,
         province_id,
         province.name,
         dt;


-------------------------每日数据加载---------------------------
insert overwrite table dws_trans_org_deliver_suc_1d
    partition (dt = '2023-01-11')
select org_id,
       org_name,
       city_id,
       city.name       city_name,
       province_id,
       province.name   province_name,
       count(order_id) order_count
from (select order_id,
             sender_district_id
      from dwd_trans_deliver_suc_detail_inc
      where dt = '2023-01-20'
      group by order_id, sender_district_id) detail
         left join
     (select id        org_id,
             org_name,
             region_id district_id
      from dim_organ_full
      where dt = '2023-01-20') organ
     on detail.sender_district_id = organ.district_id
         left join
     (select id,
             parent_id city_id
      from dim_region_full
      where dt = '2023-01-20') district
     on district_id = district.id
         left join
     (select id,
             name,
             parent_id province_id
      from dim_region_full
      where dt = '2023-01-20') city
     on city_id = city.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '2023-01-20') province
     on province_id = province.id
group by org_id,
         org_name,
         city_id,
         city.name,
         province_id,
         province.name;
