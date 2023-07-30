#! /bin/bash
#1、判断参数是否传入
if [ $# -lt 1 ]
then
	echo "必须传入all/表名..."
	exit
fi

#2、判断日期是否传入
[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)

dws_trade_org_cargo_type_order_1d_sql="
insert overwrite table dws_trade_org_cargo_type_order_1d
    partition (dt = '${datestr}')
select org_id,
       org_name,
       city_id,
       region.name city_name,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount
from (select org_id,
             org_name,
             city_id,
             cargo_type,
             cargo_type_name,
             count(order_id) order_count,
             sum(amount)     order_amount
      from (select order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id city_id,
                   sum(amount)    amount
            from (select order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         amount
                  from dwd_trade_order_detail_inc
                  where dt = '${datestr}') detail
            group by order_id,
                     cargo_type,
                     cargo_type_name,
                     sender_district_id,
                     sender_city_id) distinct_detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '${datestr}') org
           on distinct_detail.sender_district_id = org.region_id
      group by org_id,
               org_name,
               city_id,
               cargo_type,
               cargo_type_name) agg
         left join (
    select id,
           name
    from dim_region_full
    where dt = '${datestr}'
) region on city_id = region.id;
"

dws_trans_org_receive_1d_sql="
insert overwrite table dws_trans_org_receive_1d
    partition (dt = '${datestr}')
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
            where dt = '${datestr}') detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '${datestr}') organ
           on detail.sender_district_id = organ.region_id
               left join
           (select id,
                   parent_id
            from dim_region_full
            where dt = '${datestr}') district
           on region_id = district.id
               left join
           (select id   city_id,
                   name city_name,
                   parent_id
            from dim_region_full
            where dt = '${datestr}') city
           on district.parent_id = city_id
               left join
           (select id   province_id,
                   name province_name,
                   parent_id
            from dim_region_full
            where dt = '${datestr}') province
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
"

dws_trans_dispatch_1d_sql="
insert overwrite table dws_trans_dispatch_1d
    partition (dt = '${datestr}')
select count(order_id)      order_count,
       sum(distinct_amount) order_amount
from (select order_id,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail_inc
      where dt = '${datestr}'
      group by order_id) distinct_info;
"

dws_trans_org_truck_model_type_trans_finish_1d_sql="
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt = '${datestr}')
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    finish_dur_sec
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec
      from dwd_trans_trans_finish_inc
      where dt = '${datestr}') trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck_full
      where dt = '${datestr}') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name;
"

dws_trans_org_deliver_suc_1d_sql="
insert overwrite table dws_trans_org_deliver_suc_1d
    partition (dt = '${datestr}')
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
      where dt = '${datestr}'
      group by order_id, sender_district_id) detail
         left join
     (select id        org_id,
             org_name,
             region_id district_id
      from dim_organ_full
      where dt = '${datestr}') organ
     on detail.sender_district_id = organ.district_id
         left join
     (select id,
             parent_id city_id
      from dim_region_full
      where dt = '${datestr}') district
     on district_id = district.id
         left join
     (select id,
             name,
             parent_id province_id
      from dim_region_full
      where dt = '${datestr}') city
     on city_id = city.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '${datestr}') province
     on province_id = province.id
group by org_id,
         org_name,
         city_id,
         city.name,
         province_id,
         province.name;
"

dws_trans_org_sort_1d_sql="
insert overwrite table dws_trans_org_sort_1d
    partition (dt = '${datestr}')
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
      where dt = '${datestr}'
      group by org_id) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ_full
      where dt = '${datestr}') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '${datestr}') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '${datestr}') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '${datestr}') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;
"
#2、匹配表名加载数据
case $1 in
"all")
	/development/hive/bin/hive -e "use tms;${dws_trade_org_cargo_type_order_1d_sql}${dws_trans_dispatch_1d_sql}${dws_trans_org_deliver_suc_1d_sql}${dws_trans_org_receive_1d_sql}${dws_trans_org_sort_1d_sql}${dws_trans_org_truck_model_type_trans_finish_1d_sql}"
;;
"dws_trade_org_cargo_type_order_1d")
    /development/hive/bin/hive -e "use tms;${dws_trade_org_cargo_type_order_1d_sql}"
;;
"dws_trans_dispatch_1d")
    /development/hive/bin/hive -e "use tms;${dws_trans_dispatch_1d_sql}"
;;
"dws_trans_org_deliver_suc_1d")
    /development/hive/bin/hive -e "use tms;${dws_trans_org_deliver_suc_1d_sql}"
;;
"dws_trans_org_receive_1d")
    /development/hive/bin/hive -e "use tms;${dws_trans_org_receive_1d_sql}"
;;
"dws_trans_org_sort_1d")
    /development/hive/bin/hive -e "use tms;${dws_trans_org_sort_1d_sql}"
;;
"dws_trans_org_truck_model_type_trans_finish_1d")
    /development/hive/bin/hive -e "use tms;${dws_trans_org_truck_model_type_trans_finish_1d_sql}"
;;
*)
	echo "表名输入错误..."
;;
esac

