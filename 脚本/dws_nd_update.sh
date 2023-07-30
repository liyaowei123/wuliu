#! /bin/bash
#1、判断参数是否传入
if [ $# -lt 1 ]
then
	echo "必须传入all/表名..."
	exit
fi

#2、判断日期是否传入
[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)

dws_trade_org_cargo_type_order_nd_sql="
insert overwrite table dws_trade_org_cargo_type_order_nd
    partition (dt = '${datestr}')
select org_id,
       org_name,
       city_id,
       city_name,
       cargo_type,
       cargo_type_name,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('${datestr}', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         cargo_type,
         cargo_type_name,
         recent_days;
"
dws_trans_org_receive_nd_sql="
insert overwrite table dws_trans_org_receive_nd
    partition (dt = '${datestr}')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_org_receive_1d
         lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('${datestr}', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
"

dws_trans_dispatch_nd_sql="
insert overwrite table dws_trans_dispatch_nd
    partition (dt = '${datestr}')
select recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d lateral view
    explode(array(7, 30)) tmp as recent_days
where dt >= date_add('${datestr}', -recent_days + 1)
group by recent_days;
"

dws_trans_shift_trans_finish_nd_sql="
insert overwrite table dws_trans_shift_trans_finish_nd
    partition (dt = '${datestr}')
select shift_id,
       if(org_level = 1, first.region_id, city.id)     city_id,
       if(org_level = 1, first.region_name, city.name) city_name,
       org_id,
       org_name,
       line_id,
       line_name,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_model_type,
       truck_model_type_name,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count,
       trans_finish_delay_count
from (select recent_days,
             shift_id,
             line_id,
             truck_id,
             start_org_id                                       org_id,
             start_org_name                                     org_name,
             driver1_emp_id,
             driver1_name,
             driver2_emp_id,
             driver2_name,
             count(id)                                          trans_finish_count,
             sum(actual_distance)                               trans_finish_distance,
             sum(finish_dur_sec)                                trans_finish_dur_sec,
             sum(order_num)                                     trans_finish_order_count,
             sum(if(actual_end_time > estimate_end_time, 1, 0)) trans_finish_delay_count
      from dwd_trans_trans_finish_inc lateral view
          explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('${datestr}', -recent_days + 1)
      group by recent_days,
               shift_id,
               line_id,
               start_org_id,
               start_org_name,
               driver1_emp_id,
               driver1_name,
               driver2_emp_id,
               driver2_name,
               truck_id) aggregated
         left join
     (select id,
             org_level,
             region_id,
             region_name
      from dim_organ_full
      where dt = '${datestr}'
     ) first
     on aggregated.org_id = first.id
         left join
     (select id,
             parent_id
      from dim_region_full
      where dt = '${datestr}'
     ) parent
     on first.region_id = parent.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '${datestr}'
     ) city
     on parent.parent_id = city.id
         left join
     (select id,
             line_name
      from dim_shift_full
      where dt = '${datestr}') for_line_name
     on shift_id = for_line_name.id
         left join (
    select id,
           truck_model_type,
           truck_model_type_name
    from dim_truck_full
    where dt = '${datestr}'
) truck_info on truck_id = truck_info.id;
"

dws_trans_org_deliver_suc_nd_sql="
insert overwrite table dws_trans_org_deliver_suc_nd
    partition (dt = '${datestr}')
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
where dt >= date_add('${datestr}', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
"

dws_trans_org_sort_nd_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_sort_nd
    partition (dt = '${datestr}')
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
where dt >= date_add('${datestr}', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
"
#3、匹配表名加载数据
case $1 in
"all")
	/development/hive/bin/hive -e "use tms;${dws_trans_shift_trans_finish_nd_sql}${dws_trade_org_cargo_type_order_nd_sql}${dws_trans_org_sort_nd_sql}${dws_trans_dispatch_nd_sql}${dws_trans_org_receive_nd_sql}${dws_trans_org_deliver_suc_nd_sql}"
;;
"dws_trade_org_cargo_type_order_nd")
    /development/hive/bin/hive -e "use tms;${dws_trade_org_cargo_type_order_nd_sql}"
;;
"dws_trans_dispatch_nd")
    /development/hive/bin/hive -e "use tms;${dws_trans_dispatch_nd_sql}"
;;
"dws_trans_org_deliver_suc_nd")
    /development/hive/bin/hive -e "use tms;${dws_trans_org_deliver_suc_nd_sql}"
;;
"dws_trans_org_receive_nd")
    /development/hive/bin/hive -e "use tms;${dws_trans_org_receive_nd_sql}"
;;
"dws_trans_org_sort_nd")
    /development/hive/bin/hive -e "use tms;${dws_trans_org_sort_nd_sql}"
;;
"dws_trans_shift_trans_finish_nd")
    /development/hive/bin/hive -e "use tms;${dws_trans_shift_trans_finish_nd_sql}"
;;
*)
	echo "表名输入错误..."
;;
esac

