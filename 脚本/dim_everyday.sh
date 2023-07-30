#!/bin/bash

APP=tms
#1、判断参数是否传入
if [ $# -lt 1 ]
then
	echo "必须传入all/表名..."
	exit
fi

#2、判断日期是否传入,传入则加载指定日期数据,没传则加载前一天日期数据
[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)

#3、根据表名匹配加载首日数据

dim_complex_full_sql="
insert overwrite table tms.dim_complex_full
    partition (dt = '${datestr}')
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
      where dt = '${datestr}'
        and is_deleted = '0') complex_info
         join
     (select id,
             name
      from ods_base_region_info_full
      where dt = '${datestr}'
        and is_deleted = '0') dic_for_prov
     on complex_info.province_id = dic_for_prov.id
         join
     (select id,
             name
      from ods_base_region_info_full
      where dt = '${datestr}'
        and is_deleted = '0') dic_for_city
     on complex_info.city_id = dic_for_city.id
         left join
     (select collect_set(cast(courier_emp_id as string)) courier_emp_ids,
             complex_id
      from ods_express_courier_complex_full
      where dt = '${datestr}'
        and is_deleted = '0'
      group by complex_id
) complex_courier
     on complex_info.id = complex_courier.complex_id;
"

dim_organ_full_sql="
insert overwrite table tms.dim_organ_full
    partition (dt = '${datestr}')
select organ_info.id,
       organ_info.org_name,
       org_level,
       region_id,
       region_info.name        region_name,
       region_info.dict_code   region_code,
       org_parent_id,
       org_for_parent.org_name org_parent_name
from (select id,
             org_name,
             org_level,
             region_id,
             org_parent_id
      from ods_base_organ_full
      where dt = '${datestr}'
        and is_deleted = '0') organ_info
         left join (
    select id,
           name,
           dict_code
    from ods_base_region_info_full
    where dt = '${datestr}'
      and is_deleted = '0'
) region_info
                   on organ_info.region_id = region_info.id
         left join (
    select id,
           org_name
    from ods_base_organ_full
    where dt = '${datestr}'
      and is_deleted = '0'
) org_for_parent
                   on organ_info.org_parent_id = org_for_parent.id;
"

dim_region_full_sql="
insert overwrite table dim_region_full
    partition (dt = '${datestr}')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info_full
where dt = '${datestr}'
  and is_deleted = '0';
"

dim_express_courier_full_sql="
insert overwrite table tms.dim_express_courier_full
    partition (dt = '${datestr}')
select express_cor_info.id,
       emp_id,
       org_id,
       org_name,
       working_phone,
       express_type,
       dic_info.name express_type_name
from (select id,
             emp_id,
             org_id,
             md5(working_phone) working_phone,
             express_type
      from ods_express_courier_full
      where dt = '${datestr}'
        and is_deleted = '0') express_cor_info
         join (
    select id,
           org_name
    from ods_base_organ_full
    where dt = '${datestr}'
      and is_deleted = '0'
) organ_info
              on express_cor_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_base_dic_full
    where dt = '${datestr}'
      and is_deleted = '0'
) dic_info
              on express_type = dic_info.id;
"

dim_shift_full_sql="
insert overwrite table tms.dim_shift_full
    partition (dt = '${datestr}')
select shift_info.id,
       line_id,
       line_info.name line_name,
       line_no,
       line_level,
       org_id,
       transport_line_type_id,
       dic_info.name  transport_line_type_name,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       pair_line_id,
       distance,
       cost,
       estimated_time,
       start_time,
       driver1_emp_id,
       driver2_emp_id,
       truck_id,
       pair_shift_id
from (select id,
             line_id,
             start_time,
             driver1_emp_id,
             driver2_emp_id,
             truck_id,
             pair_shift_id
      from ods_line_base_shift_full
      where dt = '${datestr}'
        and is_deleted = '0') shift_info
         join
     (select id,
             name,
             line_no,
             line_level,
             org_id,
             transport_line_type_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             pair_line_id,
             distance,
             cost,
             estimated_time
      from ods_line_base_info_full
      where dt = '${datestr}'
        and is_deleted = '0') line_info
     on shift_info.line_id = line_info.id
         join (
    select id,
           name
    from ods_base_dic_full
    where dt = '${datestr}'
      and is_deleted = '0'
) dic_info on line_info.transport_line_type_id = dic_info.id;
"

dim_truck_driver_full_sql="
insert overwrite table tms.dim_truck_driver_full
    partition (dt = '${datestr}')
select driver_info.id,
       emp_id,
       org_id,
       organ_info.org_name,
       team_id,
       team_info.name team_name,
       license_type,
       init_license_date,
       expire_date,
       license_no,
       is_enabled
from (select id,
             emp_id,
             org_id,
             team_id,
             license_type,
             init_license_date,
             expire_date,
             license_no,
             is_enabled
      from ods_truck_driver_full
      where dt = '${datestr}'
        and is_deleted = '0') driver_info
         join (
    select id,
           org_name
    from ods_base_organ_full
    where dt = '${datestr}'
      and is_deleted = '0'
) organ_info
              on driver_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_truck_team_full
    where dt = '${datestr}'
      and is_deleted = '0'
) team_info
              on driver_info.team_id = team_info.id;
"

dim_truck_full_sql="
insert overwrite table tms.dim_truck_full
partition (dt = '${datestr}')
select truck_info.id,
       team_id,
       team_info.name     team_name,
       team_no,
       org_id,
       org_name,
       manager_emp_id,
       truck_no,
       truck_model_id,
       model_name         truck_model_name,
       model_type         truck_model_type,
       dic_for_type.name  truck_model_type_name,
       model_no           truck_model_no,
       brand              truck_brand,
       dic_for_brand.name truck_brand_name,
       truck_weight,
       load_weight,
       total_weight,
       eev,
       boxcar_len,
       boxcar_wd,
       boxcar_hg,
       max_speed,
       oil_vol,
       device_gps_id,
       engine_no,
       license_registration_date,
       license_last_check_date,
       license_expire_date,
       is_enabled
from (select id,
             team_id,

             md5(truck_no) truck_no,
             truck_model_id,

             device_gps_id,
             engine_no,
             license_registration_date,
             license_last_check_date,
             license_expire_date,
             is_enabled
      from ods_truck_info_full
      where dt = '${datestr}'
        and is_deleted = '0') truck_info
         join
     (select id,
             name,
             team_no,
             org_id,

             manager_emp_id
      from ods_truck_team_full
      where dt = '${datestr}'
        and is_deleted = '0') team_info
     on truck_info.team_id = team_info.id
         join
     (select id,
             model_name,
             model_type,

             model_no,
             brand,

             truck_weight,
             load_weight,
             total_weight,
             eev,
             boxcar_len,
             boxcar_wd,
             boxcar_hg,
             max_speed,
             oil_vol
      from ods_truck_model_full
      where dt = '${datestr}'
        and is_deleted = '0') model_info
     on truck_info.truck_model_id = model_info.id
         join
     (select id,
             org_name
      from ods_base_organ_full
      where dt = '${datestr}'
        and is_deleted = '0'
     ) organ_info
     on org_id = organ_info.id
         join
     (select id,
             name
      from ods_base_dic_full
      where dt = '${datestr}'
        and is_deleted = '0') dic_for_type
     on model_info.model_type = dic_for_type.id
         join
     (select id,
             name
      from ods_base_dic_full
      where dt = '${datestr}'
        and is_deleted = '0') dic_for_brand
     on model_info.brand = dic_for_brand.id;
"

dim_user_zip_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_zip
    partition (dt)
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       start_date,
       if(rk = 1, end_date, date_add('${datestr}', -1)) end_date,
       if(rk = 1, end_date, date_add('${datestr}', -1)) dt
from (select id,
             login_name,
             nick_name,
             passwd,
             real_name,
             phone_num,
             email,
             user_level,
             birthday,
             gender,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
      from (select id,
                   login_name,
                   nick_name,
                   passwd,
                   real_name,
                   phone_num,
                   email,
                   user_level,
                   birthday,
                   gender,
                   start_date,
                   end_date
            from dim_user_zip
            where dt = '9999-12-31'
            union
            select id,
                   login_name,
                   nick_name,
                   md5(passwd)                                                                              passwd,
                   md5(real_name)                                                                           realname,
                   md5(if(phone_num regexp
                          '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
                          phone_num, null))                                                                 phone_num,
                   md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', email, null)) email,
                   user_level,
                   cast(date_add('1970-01-01', cast(birthday as int)) as string)                            birthday,
                   gender,
                   '${datestr}'                                                                             start_date,
                   '9999-12-31'                                                                             end_date
            from (select after.id,
                         after.login_name,
                         after.nick_name,
                         after.passwd,
                         after.real_name,
                         after.phone_num,
                         after.email,
                         after.user_level,
                         after.birthday,
                         after.gender,
                         row_number() over (partition by after.id order by ts desc) rn
                  from ods_user_info_inc
                  where dt = '${datestr}'
                    and after.is_deleted = '0'
                 ) inc
            where rn = 1) full_info) final_info;
"

dim_user_address_zip_sql="
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
       if(rk = 1, end_date, date_add('${datestr}', -1)) end_date,
       if(rk = 1, end_date, date_add('${datestr}', -1)) dt
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
                   '${datestr}' start_date,
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
                  where dt = '${datestr}'
                    and after.is_deleted = '0') inc
            where rn = 1
           ) union_info
     ) with_rk;
"

case $1 in
"all")
	/development/hive/bin/hive -e "use tms;${dim_complex_full_sql}${dim_express_courier_full_sql}${dim_organ_full_sql}${dim_region_full_sql}${dim_shift_full_sql}${dim_truck_driver_full_sql}${dim_truck_full_sql}${dim_user_address_zip_sql}${dim_user_zip_sql}"
;;
"dim_complex_full")
    /development/hive/bin/hive -e "use tms;${dim_complex_full_sql}"
;;
"dim_express_courier_full")
    /development/hive/bin/hive -e "use tms;${dim_express_courier_full_sql}"
;;
"dim_organ_full")
    /development/hive/bin/hive -e "use tms;${dim_organ_full_sql}"
;;
"dim_region_full")
    /development/hive/bin/hive -e "use tms;${dim_region_full_sql}"
;;
"dim_shift_full")
    /development/hive/bin/hive -e "use tms;${dim_shift_full_sql}"
;;
"dim_truck_driver_full")
    /development/hive/bin/hive -e "use tms;${dim_truck_driver_full_sql}"
;;
"dim_truck_full")
    /development/hive/bin/hive -e "use tms;${dim_truck_full_sql}"
;;
"dim_user_address_zip")
    /development/hive/bin/hive -e "use tms;${dim_user_address_zip_sql}"
;;
"dim_user_zip")
    /development/hive/bin/hive -e "use tms;${dim_user_zip_sql}"
;;
*)
	echo "表名输入错误..."
;;
esac

