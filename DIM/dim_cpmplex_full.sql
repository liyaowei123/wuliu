drop table if exists dim_complex_full;
create external table dim_complex_full(
                                          `id` bigint comment '小区ID',
                                          `complex_name` string comment '小区名称',
                                          `courier_emp_ids` array<string> comment '负责快递员IDS',
                                          `province_id` bigint comment '省份ID',
                                          `province_name` string comment '省份名称',
                                          `city_id` bigint comment '城市ID',
                                          `city_name` string comment '城市名称',
                                          `district_id` bigint comment '区（县）ID',
                                          `district_name` string comment '区（县）名称'
) comment '小区维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/DIM/dim_complex_full';


insert overwrite table tms.dim_complex_full
    partition (dt = '2023-01-20')
select complex_info.id   id,
       complex_name,
       courier_emp_ids,
       province_id,
       dic_for_prov.name province_name,
       city_id,
       dic_for_city.name city_name,
       district_id,
       district_name
from (select id,
             complex_name,
             province_id,
             city_id,
             district_id,
             district_name
      from ods_base_complex_full
      where dt = '2023-01-20'
        and is_deleted = '0') complex_info
         join
     (select id,
             name
      from ods_base_region_info_full
      where dt = '2023-01-20'
        and is_deleted = '0') dic_for_prov
     on complex_info.province_id = dic_for_prov.id
         join
     (select id,
             name
      from ods_base_region_info_full
      where dt = '2023-01-20'
        and is_deleted = '0') dic_for_city
     on complex_info.city_id = dic_for_city.id
         left join
     (select
          collect_set(cast(courier_emp_id as string)) courier_emp_ids,
          complex_id
      from ods_express_courier_complex_full where dt='2023-01-20' and is_deleted='0'
      group by complex_id
     ) complex_courier
     on complex_info.id = complex_courier.complex_id;






