drop table if exists dim_user_address_zip;
create external table dim_user_address_zip(
                                              `id` bigint COMMENT '地址ID',
                                              `user_id` bigint COMMENT '用户ID',
                                              `phone` string COMMENT '电话号',
                                              `province_id` bigint COMMENT '所属省份ID',
                                              `city_id` bigint COMMENT '所属城市ID',
                                              `district_id` bigint COMMENT '所属区县ID',
                                              `complex_id` bigint COMMENT '所属小区ID',
                                              `address` string COMMENT '详细地址',
                                              `is_default` tinyint COMMENT '是否默认',
                                              `start_date` string COMMENT '起始日期',
                                              `end_date` string COMMENT '结束日期'
) comment '用户地址拉链表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/DIM/dim_user_address_zip'
    tblproperties('orc.compress'='snappy');


----------首日数据加载----------
insert overwrite table dim_user_address_zip
    partition (dt = '9999-12-31')
select after.id,
       after.user_id,
       md5(if(after.phone regexp
              '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              after.phone, null))               phone,
       after.province_id,
       after.city_id,
       after.district_id,
       after.complex_id,
       after.address,
       after.is_default,
       concat(substr(after.create_time, 1, 10), ' ',
              substr(after.create_time, 12, 8)) start_date,
       '9999-12-31'                             end_date
from ods_user_address_inc
where dt = '2023-01-20'
  and after.is_deleted = '0';

----------每日数据加载----------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_address_zip
    partition (dt)
select id,
       user_id,
       phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       start_date,
       if(rk = 1, end_date, date_add('2023-01-21', -1)) end_date,
       if(rk = 1, end_date, date_add('2023-01-21', -1)) dt
from (select id,
             user_id,
             phone,
             province_id,
             city_id,
             district_id,
             complex_id,
             address,
             is_default,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
      from (select id,
                   user_id,
                   phone,
                   province_id,
                   city_id,
                   district_id,
                   complex_id,
                   address,
                   is_default,
                   start_date,
                   end_date
            from dim_user_address_zip
            where dt = '9999-12-31'
            union
            select id,
                   user_id,
                   phone,
                   province_id,
                   city_id,
                   district_id,
                   complex_id,
                   address,
                   is_default,
                   '2023-01-21' start_date,
                   '9999-12-31' end_date
            from (select after.id,
                         after.user_id,
                         after.phone,
                         after.province_id,
                         after.city_id,
                         after.district_id,
                         after.complex_id,
                         after.address,
                         cast(after.is_default as tinyint)                          is_default,
                         row_number() over (partition by after.id order by ts desc) rn
                  from ods_user_address_inc
                  where dt = '2023-01-21'
                    and after.is_deleted = '0') inc
            where rn = 1
           ) union_info
     ) with_rk;