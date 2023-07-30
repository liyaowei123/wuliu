drop table if exists dws_trans_org_sort_1d;
create external table dws_trans_org_sort_1d(
                                               `org_id` bigint comment '机构ID',
                                               `org_name` string comment '机构名称',
                                               `city_id` bigint comment '城市ID',
                                               `city_name` string comment '城市名称',
                                               `province_id` bigint comment '省份ID',
                                               `province_name` string comment '省份名称',
                                               `sort_count` bigint comment '分拣次数'
) comment '物流域机构粒度分拣 1 日汇总表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dws/dws_trans_org_sort_1d/'
    tblproperties('orc.compress'='snappy');

--------------------------首日数据加载-----------------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_sort_1d
    partition (dt)
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
       sort_count,
       dt
from (select org_id,
             count(*) sort_count,
             dt
      from dwd_bound_sort_inc
      group by org_id, dt) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ_full
      where dt = '2023-01-20') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '2023-01-20') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '2023-01-20') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '2023-01-20') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;


---------------------------每日数据加载------------------------------
insert overwrite table dws_trans_org_sort_1d
    partition (dt = '2023-01-11')
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
       sort_count
from (select org_id,
             count(*) sort_count
      from dwd_bound_sort_inc
      where dt = '2023-01-20'
      group by org_id) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ_full
      where dt = '2023-01-20') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '2023-01-20') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '2023-01-20') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '2023-01-20') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;
