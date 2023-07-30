#! /bin/bash
#1、判断参数是否传入
if [ $# -lt 1 ]
then
	echo "必须传入all/表名..."
	exit
fi

#2、判断日期是否传入
[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)

ads_trans_order_stats_sql="
insert overwrite table ads_trans_order_stats
select dt,
       recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from ads_trans_order_stats
union
select '${datestr}'                                         dt,
       nvl(receive_1d.recent_days, dispatch_1d.recent_days) recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from (select 1                 recent_days,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '${datestr}') receive_1d
         full outer join
     (select 1            recent_days,
             order_count  dispatch_order_count,
             order_amount dispatch_order_amount
      from dws_trans_dispatch_1d
      where dt = '${datestr}') dispatch_1d
     on receive_1d.recent_days = dispatch_1d.recent_days
union
select '${datestr}'                                         dt,
       nvl(receive_nd.recent_days, dispatch_nd.recent_days) recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from (select recent_days,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '${datestr}'
      group by recent_days) receive_nd
         full outer join
     (select recent_days,
             order_count  dispatch_order_count,
             order_amount dispatch_order_amount
      from dws_trans_dispatch_nd
      where dt = '${datestr}') dispatch_nd
     on receive_nd.recent_days = dispatch_nd.recent_days;
"

ads_trans_stats_sql="
insert overwrite table ads_trans_stats
select dt,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec
from ads_trans_stats
union
select '${datestr}'               dt,
       1                          recent_days,
       sum(trans_finish_count)    trans_finish_count,
       sum(trans_finish_distance) trans_finish_distance,
       sum(trans_finish_dur_sec)  trans_finish_dur_sec
from dws_trans_org_truck_model_type_trans_finish_1d
where dt = '${datestr}'
union
select '${datestr}'               dt,
       recent_days,
       sum(trans_finish_count)    trans_finish_count,
       sum(trans_finish_distance) trans_finish_distance,
       sum(trans_finish_dur_sec)  trans_finish_dur_sec
from dws_trans_shift_trans_finish_nd
where dt = '${datestr}'
group by recent_days;
"
ads_trans_order_stats_td_sql="
insert overwrite table ads_trans_order_stats_td
select dt,
       bounding_order_count,
       bounding_order_amount
from ads_trans_order_stats_td
union
select dt,
       sum(order_count)  bounding_order_count,
       sum(order_amount) bounding_order_amount
from (select dt,
             order_count,
             order_amount
      from dws_trans_dispatch_td
      where dt = '${datestr}'
      union
      select dt,
             order_count * (-1),
             order_amount * (-1)
      from dws_trans_bound_finish_td
      where dt = '${datestr}') new
group by dt;
"

ads_order_stats_sql="
insert overwrite table ads_order_stats
select dt,
       recent_days,
       order_count,
       order_amount
from ads_order_stats
union
select '${datestr}'      dt,
       1                 recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d
where dt = '${datestr}'
union
select '${datestr}'      dt,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_nd
where dt = '${datestr}'
group by recent_days;
"

ads_order_cargo_type_stats_sql="
insert overwrite table ads_order_cargo_type_stats
select dt,
       recent_days,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount
from ads_order_cargo_type_stats
union
select '${datestr}'      dt,
       1                 recent_days,
       cargo_type,
       cargo_type_name,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d
where dt = '${datestr}'
group by cargo_type,
         cargo_type_name
union
select '${datestr}'      dt,
       recent_days,
       cargo_type,
       cargo_type_name,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_nd
where dt = '${datestr}'
group by cargo_type,
         cargo_type_name,
         recent_days;
"

ads_city_stats_sql="
insert overwrite table ads_city_stats
select dt,
       recent_days,
       city_id,
       city_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from ads_city_stats
union
select nvl(city_order_1d.dt, city_trans_1d.dt)                   dt,
       nvl(city_order_1d.recent_days, city_trans_1d.recent_days) recent_days,
       nvl(city_order_1d.city_id, city_trans_1d.city_id)         city_id,
       nvl(city_order_1d.city_name, city_trans_1d.city_name)     city_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '${datestr}'      dt,
             1                 recent_days,
             city_id,
             city_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_1d
      where dt = '${datestr}'
      group by city_id,
               city_name) city_order_1d
         full outer join
     (select '${datestr}'                                         dt,
             1                                                    recent_days,
             city_id,
             city_name,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from (select if(org_level = 1, city_for_level1.id, city_for_level2.id)     city_id,
                   if(org_level = 1, city_for_level1.name, city_for_level2.name) city_name,
                   trans_finish_count,
                   trans_finish_distance,
                   trans_finish_dur_sec
            from (select org_id,
                         trans_finish_count,
                         trans_finish_distance,
                         trans_finish_dur_sec
                  from dws_trans_org_truck_model_type_trans_finish_1d
                  where dt = '${datestr}') trans_origin
                     left join
                 (select id,
                         org_level,
                         region_id
                  from dim_organ_full
                  where dt = '${datestr}') organ
                 on org_id = organ.id
                     left join
                 (select id,
                         name,
                         parent_id
                  from dim_region_full
                  where dt = '${datestr}') city_for_level1
                 on region_id = city_for_level1.id
                     left join
                 (select id,
                         name
                  from dim_region_full
                  where dt = '${datestr}') city_for_level2
                 on city_for_level1.parent_id = city_for_level2.id) trans_1d
      group by city_id,
               city_name) city_trans_1d
     on city_order_1d.dt = city_trans_1d.dt
         and city_order_1d.recent_days = city_trans_1d.recent_days
         and city_order_1d.city_id = city_trans_1d.city_id
         and city_order_1d.city_name = city_trans_1d.city_name
union
select nvl(city_order_nd.dt, city_trans_nd.dt)                   dt,
       nvl(city_order_nd.recent_days, city_trans_nd.recent_days) recent_days,
       nvl(city_order_nd.city_id, city_trans_nd.city_id)         city_id,
       nvl(city_order_nd.city_name, city_trans_nd.city_name)     city_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '${datestr}'      dt,
             recent_days,
             city_id,
             city_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_nd
      where dt = '${datestr}'
      group by city_id,
               city_name,
               recent_days) city_order_nd
         full outer join
     (select '${datestr}'                                         dt,
             city_id,
             city_name,
             recent_days,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from dws_trans_shift_trans_finish_nd
      where dt = '${datestr}'
      group by city_id,
               city_name,
               recent_days
     ) city_trans_nd
     on city_order_nd.dt = city_trans_nd.dt
         and city_order_nd.recent_days = city_trans_nd.recent_days
         and city_order_nd.city_id = city_trans_nd.city_id
         and city_order_nd.city_name = city_trans_nd.city_name;
"

ads_org_stats_sql="
insert overwrite table ads_org_stats
select dt,
       recent_days,
       org_id,
       org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from ads_org_stats
union
select nvl(org_order_1d.dt, org_trans_1d.dt)                   dt,
       nvl(org_order_1d.recent_days, org_trans_1d.recent_days) recent_days,
       nvl(org_order_1d.org_id, org_trans_1d.org_id)           org_id,
       nvl(org_order_1d.org_name, org_trans_1d.org_name)       org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '${datestr}'      dt,
             1                 recent_days,
             org_id,
             org_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_1d
      where dt = '${datestr}'
      group by org_id,
               org_name) org_order_1d
         full outer join
     (select '${datestr}'                                         dt,
             org_id,
             org_name,
             1                                                    recent_days,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from dws_trans_org_truck_model_type_trans_finish_1d
      where dt = '${datestr}'
      group by org_id,
               org_name
     ) org_trans_1d
     on org_order_1d.dt = org_trans_1d.dt
         and org_order_1d.recent_days = org_trans_1d.recent_days
         and org_order_1d.org_id = org_trans_1d.org_id
         and org_order_1d.org_name = org_trans_1d.org_name
union
select org_order_nd.dt,
       org_order_nd.recent_days,
       org_order_nd.org_id,
       org_order_nd.org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '${datestr}'      dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_nd
      where dt = '${datestr}'
      group by org_id,
               org_name,
               recent_days) org_order_nd
         join
     (select '${datestr}'                                         dt,
             recent_days,
             org_id,
             org_name,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from dws_trans_shift_trans_finish_nd
      where dt = '${datestr}'
      group by org_id,
               org_name,
               recent_days
     ) org_trans_nd
     on org_order_nd.dt = org_trans_nd.dt
         and org_order_nd.recent_days = org_trans_nd.recent_days
         and org_order_nd.org_id = org_trans_nd.org_id
         and org_order_nd.org_name = org_trans_nd.org_name;
"

ads_shift_stats_sql="
insert overwrite table ads_shift_stats
select dt,
       recent_days,
       shift_id,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from ads_shift_stats
union
select '${datestr}' dt,
       recent_days,
       shift_id,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from dws_trans_shift_trans_finish_nd
where dt = '${datestr}';
"

ads_line_stats_sql="
insert overwrite table ads_line_stats
select dt,
       recent_days,
       line_id,
       line_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from ads_line_stats
union
select '${datestr}'                  dt,
       recent_days,
       line_id,
       line_name,
       sum(trans_finish_count)       trans_finish_count,
       sum(trans_finish_distance)    trans_finish_distance,
       sum(trans_finish_dur_sec)     trans_finish_dur_sec,
       sum(trans_finish_order_count) trans_finish_order_count
from dws_trans_shift_trans_finish_nd
where dt = '${datestr}'
group by line_id,
         line_name,
         recent_days;
"

ads_driver_stats_sql="
insert overwrite table ads_driver_stats
select dt,
       recent_days,
       driver_emp_id,
       driver_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec,
       trans_finish_late_count
from ads_driver_stats
union
select '${datestr}'                                         dt,
       recent_days,
       driver_id,
       driver_name,
       sum(trans_finish_count)                              trans_finish_count,
       sum(trans_finish_distance)                           trans_finish_distance,
       sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec,
       sum(trans_finish_delay_count)                        trans_finish_delay_count
from (select recent_days,
             driver1_emp_id driver_id,
             driver1_name   driver_name,
             trans_finish_count,
             trans_finish_distance,
             trans_finish_dur_sec,
             trans_finish_delay_count
      from dws_trans_shift_trans_finish_nd
      where dt = '${datestr}'
        and driver2_emp_id is null
      union
      select recent_days,
             cast(driver_info[0] as bigint) driver_id,
             driver_info[1] driver_name,
             trans_finish_count,
             trans_finish_distance,
             trans_finish_dur_sec,
             trans_finish_delay_count
      from (select recent_days,
                   array(array(driver1_emp_id, driver1_name),
                         array(driver2_emp_id, driver2_name)) driver_arr,
                   trans_finish_count,
                   trans_finish_distance / 2                  trans_finish_distance,
                   trans_finish_dur_sec / 2                   trans_finish_dur_sec,
                   trans_finish_delay_count
            from dws_trans_shift_trans_finish_nd
            where dt = '${datestr}'
              and driver2_emp_id is not null) t1
               lateral view explode(driver_arr) tmp as driver_info) t2
group by driver_id,
         driver_name,
         recent_days;
"

ads_truck_stats_sql="
insert overwrite table ads_truck_stats
select dt,
       recent_days,
       truck_model_type,
       truck_model_type_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from ads_truck_stats
union
select '${datestr}'                                         dt,
       recent_days,
       truck_model_type,
       truck_model_type_name,
       sum(trans_finish_count)                              trans_finish_count,
       sum(trans_finish_distance)                           trans_finish_distance,
       sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
from dws_trans_shift_trans_finish_nd
where dt = '${datestr}'
group by truck_model_type,
         truck_model_type_name,
         recent_days;
"

ads_express_stats_sql="
insert overwrite table ads_truck_stats
select dt,
       recent_days,
       truck_model_type,
       truck_model_type_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from ads_truck_stats
union
select '${datestr}'                                         dt,
       recent_days,
       truck_model_type,
       truck_model_type_name,
       sum(trans_finish_count)                              trans_finish_count,
       sum(trans_finish_distance)                           trans_finish_distance,
       sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
from dws_trans_shift_trans_finish_nd
where dt = '${datestr}'
group by truck_model_type,
         truck_model_type_name,
         recent_days;insert overwrite table ads_express_stats
select dt,
       recent_days,
       deliver_suc_count,
       sort_count
from ads_express_stats
union
select nvl(deliver_1d.dt, sort_1d.dt)                   dt,
       nvl(deliver_1d.recent_days, sort_1d.recent_days) recent_days,
       deliver_suc_count,
       sort_count
from (select '${datestr}'     dt,
             1                recent_days,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '${datestr}') deliver_1d
         full outer join
     (select '${datestr}'    dt,
             1               recent_days,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '${datestr}') sort_1d
     on deliver_1d.dt = sort_1d.dt
         and deliver_1d.recent_days = sort_1d.recent_days
union
select nvl(deliver_nd.dt, sort_nd.dt)                   dt,
       nvl(deliver_nd.recent_days, sort_nd.recent_days) recent_days,
       deliver_suc_count,
       sort_count
from (select '${datestr}'     dt,
             recent_days,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '${datestr}'
      group by recent_days) deliver_nd
         full outer join
     (select '${datestr}'    dt,
             recent_days,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '${datestr}'
      group by recent_days) sort_nd
     on deliver_nd.dt = sort_nd.dt
         and deliver_nd.recent_days = sort_nd.recent_days;
"

ads_express_province_stats_sql="
insert overwrite table ads_express_province_stats
select dt,
       recent_days,
       province_id,
       province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from ads_express_province_stats
union
select nvl(nvl(province_deliver_1d.dt, province_sort_1d.dt), province_receive_1d.dt) dt,
       nvl(nvl(province_deliver_1d.recent_days, province_sort_1d.recent_days),
           province_receive_1d.recent_days)                                          recent_days,
       nvl(nvl(province_deliver_1d.province_id, province_sort_1d.province_id),
           province_receive_1d.province_id)                                          province_id,
       nvl(nvl(province_deliver_1d.province_name, province_sort_1d.province_name),
           province_receive_1d.province_name)                                        province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '${datestr}'     dt,
             1                recent_days,
             province_id,
             province_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '${datestr}'
      group by province_id,
               province_name) province_deliver_1d
         full outer join
     (select '${datestr}'    dt,
             1               recent_days,
             province_id,
             province_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '${datestr}'
      group by province_id,
               province_name) province_sort_1d
     on province_deliver_1d.dt = province_sort_1d.dt
         and province_deliver_1d.recent_days = province_sort_1d.recent_days
         and province_deliver_1d.province_id = province_sort_1d.province_id
         and province_deliver_1d.province_name = province_sort_1d.province_name
         full outer join
     (select '${datestr}'      dt,
             1                 recent_days,
             province_id,
             province_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '${datestr}'
      group by province_id,
               province_name) province_receive_1d
     on province_deliver_1d.dt = province_receive_1d.dt
         and province_deliver_1d.recent_days = province_receive_1d.recent_days
         and province_deliver_1d.province_id = province_receive_1d.province_id
         and province_deliver_1d.province_name = province_receive_1d.province_name
union
select nvl(nvl(province_deliver_nd.dt, province_sort_nd.dt), province_receive_nd.dt) dt,
       nvl(nvl(province_deliver_nd.recent_days, province_sort_nd.recent_days),
           province_receive_nd.recent_days)                                          recent_days,
       nvl(nvl(province_deliver_nd.province_id, province_sort_nd.province_id),
           province_receive_nd.province_id)                                          province_id,
       nvl(nvl(province_deliver_nd.province_name, province_sort_nd.province_name),
           province_receive_nd.province_name)                                        province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '${datestr}'     dt,
             recent_days,
             province_id,
             province_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '${datestr}'
      group by recent_days,
               province_id,
               province_name) province_deliver_nd
         full outer join
     (select '${datestr}'    dt,
             recent_days,
             province_id,
             province_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '${datestr}'
      group by recent_days,
               province_id,
               province_name) province_sort_nd
     on province_deliver_nd.dt = province_sort_nd.dt
         and province_deliver_nd.recent_days = province_sort_nd.recent_days
         and province_deliver_nd.province_id = province_sort_nd.province_id
         and province_deliver_nd.province_name = province_sort_nd.province_name
         full outer join
     (select '${datestr}'      dt,
             recent_days,
             province_id,
             province_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '${datestr}'
      group by recent_days,
               province_id,
               province_name) province_receive_nd
     on province_deliver_nd.dt = province_receive_nd.dt
         and province_deliver_nd.recent_days = province_receive_nd.recent_days
         and province_deliver_nd.province_id = province_receive_nd.province_id
         and province_deliver_nd.province_name = province_receive_nd.province_name;
"

ads_express_city_stats_sql="
insert overwrite table ads_express_city_stats
select dt,
       recent_days,
       city_id,
       city_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from ads_express_city_stats
union
select nvl(nvl(city_deliver_1d.dt, city_sort_1d.dt), city_receive_1d.dt) dt,
       nvl(nvl(city_deliver_1d.recent_days, city_sort_1d.recent_days),
           city_receive_1d.recent_days)                                  recent_days,
       nvl(nvl(city_deliver_1d.city_id, city_sort_1d.city_id),
           city_receive_1d.city_id)                                      city_id,
       nvl(nvl(city_deliver_1d.city_name, city_sort_1d.city_name),
           city_receive_1d.city_name)                                    city_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '${datestr}'     dt,
             1                recent_days,
             city_id,
             city_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '${datestr}'
      group by city_id,
               city_name) city_deliver_1d
         full outer join
     (select '${datestr}'    dt,
             1               recent_days,
             city_id,
             city_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '${datestr}'
      group by city_id,
               city_name) city_sort_1d
     on city_deliver_1d.dt = city_sort_1d.dt
         and city_deliver_1d.recent_days = city_sort_1d.recent_days
         and city_deliver_1d.city_id = city_sort_1d.city_id
         and city_deliver_1d.city_name = city_sort_1d.city_name
         full outer join
     (select '${datestr}'      dt,
             1                 recent_days,
             city_id,
             city_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '${datestr}'
      group by city_id,
               city_name) city_receive_1d
     on city_deliver_1d.dt = city_receive_1d.dt
         and city_deliver_1d.recent_days = city_receive_1d.recent_days
         and city_deliver_1d.city_id = city_receive_1d.city_id
         and city_deliver_1d.city_name = city_receive_1d.city_name
union
select nvl(nvl(city_deliver_nd.dt, city_sort_nd.dt), city_receive_nd.dt) dt,
       nvl(nvl(city_deliver_nd.recent_days, city_sort_nd.recent_days),
           city_receive_nd.recent_days)                                  recent_days,
       nvl(nvl(city_deliver_nd.city_id, city_sort_nd.city_id),
           city_receive_nd.city_id)                                      city_id,
       nvl(nvl(city_deliver_nd.city_name, city_sort_nd.city_name),
           city_receive_nd.city_name)                                    city_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '${datestr}'     dt,
             recent_days,
             city_id,
             city_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '${datestr}'
      group by recent_days,
               city_id,
               city_name) city_deliver_nd
         full outer join
     (select '${datestr}'    dt,
             recent_days,
             city_id,
             city_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '${datestr}'
      group by recent_days,
               city_id,
               city_name) city_sort_nd
     on city_deliver_nd.dt = city_sort_nd.dt
         and city_deliver_nd.recent_days = city_sort_nd.recent_days
         and city_deliver_nd.city_id = city_sort_nd.city_id
         and city_deliver_nd.city_name = city_sort_nd.city_name
         full outer join
     (select '${datestr}'      dt,
             recent_days,
             city_id,
             city_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '${datestr}'
      group by recent_days,
               city_id,
               city_name) city_receive_nd
     on city_deliver_nd.dt = city_receive_nd.dt
         and city_deliver_nd.recent_days = city_receive_nd.recent_days
         and city_deliver_nd.city_id = city_receive_nd.city_id
         and city_deliver_nd.city_name = city_receive_nd.city_name;
"

ads_express_org_stats_sql="
insert overwrite table ads_express_org_stats
select dt,
       recent_days,
       org_id,
       org_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from ads_express_org_stats
union
select nvl(nvl(org_deliver_1d.dt, org_sort_1d.dt), org_receive_1d.dt) dt,
       nvl(nvl(org_deliver_1d.recent_days, org_sort_1d.recent_days),
           org_receive_1d.recent_days)                                recent_days,
       nvl(nvl(org_deliver_1d.org_id, org_sort_1d.org_id),
           org_receive_1d.org_id)                                     org_id,
       nvl(nvl(org_deliver_1d.org_name, org_sort_1d.org_name),
           org_receive_1d.org_name)                                   org_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '${datestr}'     dt,
             1                recent_days,
             org_id,
             org_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '${datestr}'
      group by org_id,
               org_name) org_deliver_1d
         full outer join
     (select '${datestr}'    dt,
             1               recent_days,
             org_id,
             org_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '${datestr}'
      group by org_id,
               org_name) org_sort_1d
     on org_deliver_1d.dt = org_sort_1d.dt
         and org_deliver_1d.recent_days = org_sort_1d.recent_days
         and org_deliver_1d.org_id = org_sort_1d.org_id
         and org_deliver_1d.org_name = org_sort_1d.org_name
         full outer join
     (select '${datestr}'      dt,
             1                 recent_days,
             org_id,
             org_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '${datestr}'
      group by org_id,
               org_name) org_receive_1d
     on org_deliver_1d.dt = org_receive_1d.dt
         and org_deliver_1d.recent_days = org_receive_1d.recent_days
         and org_deliver_1d.org_id = org_receive_1d.org_id
         and org_deliver_1d.org_name = org_receive_1d.org_name
union
select nvl(nvl(org_deliver_nd.dt, org_sort_nd.dt), org_receive_nd.dt) dt,
       nvl(nvl(org_deliver_nd.recent_days, org_sort_nd.recent_days),
           org_receive_nd.recent_days)                                recent_days,
       nvl(nvl(org_deliver_nd.org_id, org_sort_nd.org_id),
           org_receive_nd.org_id)                                     org_id,
       nvl(nvl(org_deliver_nd.org_name, org_sort_nd.org_name),
           org_receive_nd.org_name)                                   org_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '${datestr}'     dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '${datestr}'
      group by recent_days,
               org_id,
               org_name) org_deliver_nd
         full outer join
     (select '${datestr}'    dt,
             recent_days,
             org_id,
             org_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '${datestr}'
      group by recent_days,
               org_id,
               org_name) org_sort_nd
     on org_deliver_nd.dt = org_sort_nd.dt
         and org_deliver_nd.recent_days = org_sort_nd.recent_days
         and org_deliver_nd.org_id = org_sort_nd.org_id
         and org_deliver_nd.org_name = org_sort_nd.org_name
         full outer join
     (select '${datestr}'      dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '${datestr}'
      group by recent_days,
               org_id,
               org_name) org_receive_nd
     on org_deliver_nd.dt = org_receive_nd.dt
         and org_deliver_nd.recent_days = org_receive_nd.recent_days
         and org_deliver_nd.org_id = org_receive_nd.org_id
         and org_deliver_nd.org_name = org_receive_nd.org_name;
"
#3、匹配表名加载数据
case $1 in
"all")
	/development/hive/bin/hive -e "use tms;${ads_city_stats_sql}${ads_driver_stats_sql}${ads_express_city_stats_sql}${ads_express_org_stats_sql}${ads_express_province_stats_sql}${ads_express_stats_sql}${ads_line_stats_sql}${ads_order_cargo_type_stats_sql}${ads_order_stats_sql}${ads_org_stats_sql}${ads_shift_stats_sql}${ads_trans_order_stats_sql}${ads_trans_order_stats_td_sql}${ads_trans_stats_sql}${ads_truck_stats_sql}"
;;
"ads_city_stats")
    /development/hive/bin/hive -e "use tms;${ads_city_stats_sql}"
;;
"ads_driver_stats")
    /development/hive/bin/hive -e "use tms;${ads_driver_stats_sql}"
;;
"ads_express_city_stats")
    /development/hive/bin/hive -e "use tms;${ads_express_city_stats_sql}"
;;
"ads_express_org_stats")
    /development/hive/bin/hive -e "use tms;${ads_express_org_stats_sql}"
;;
"ads_express_province_stats")
    /development/hive/bin/hive -e "use tms;${ads_express_province_stats_sql}"
;;
"ads_express_stats")
    /development/hive/bin/hive -e "use tms;${ads_express_stats_sql}"
;;
"ads_line_stats")
    /development/hive/bin/hive -e "use tms;${ads_line_stats_sql}"
;;
"ads_order_cargo_type_stats")
    /development/hive/bin/hive -e "use tms;${ads_order_cargo_type_stats_sql}"
;;
"ads_order_stats")
    /development/hive/bin/hive -e "use tms;${ads_order_stats_sql}"
;;
"ads_org_stats")
    /development/hive/bin/hive -e "use tms;${ads_org_stats_sql}"
;;
"ads_shift_stats")
    /development/hive/bin/hive -e "use tms;${ads_shift_stats_sql}"
;;
"ads_trans_order_stats")
    /development/hive/bin/hive -e "use tms;${ads_trans_order_stats_sql}"
;;
"ads_trans_order_stats_td")
    /development/hive/bin/hive -e "use tms;${ads_trans_order_stats_td_sql}"
;;
"ads_trans_stats")
    /development/hive/bin/hive -e "use tms;${ads_trans_stats_sql}"
;;
"ads_truck_stats")
    /development/hive/bin/hive -e "use tms;${ads_truck_stats_sql}"
;;
*)
	echo "表名输入错误..."
;;
esac

