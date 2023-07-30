#!/bin/bash

APP='tms'

if [ -n "$2" ] ;then
   do_date=$2
else 
   do_date=`date -d '-1 day' +%F`
fi

load_data(){
    sql=""
    for i in $*; do
        #判断路径是否存在
        hadoop fs -test -e /origin_data/tms/${i:4}/$do_date
        #路径存在方可装载数据
        if [[ $? = 0 ]]; then
            sql=$sql"load data inpath '/origin_data/tms/${i:4}/$do_date' OVERWRITE into table ${APP}.${i} partition(dt='$do_date');"
        fi
    done
    hive -e "$sql"
}

case $1 in
    ods_order_info_inc | ods_order_cargo_inc | ods_transport_task_inc | ods_order_org_bound_inc | ods_user_info_inc | ods_user_address_inc | ods_base_complex_full | ods_base_dic_full | ods_base_region_info_full | ods_base_organ_full | ods_express_courier_full | ods_express_courier_complex_full | ods_employee_info_full | ods_line_base_shift_full | ods_line_base_info_full | ods_truck_driver_full | ods_truck_info_full | ods_truck_model_full | ods_truck_team_full)
        load_data $1
    ;;
    "all")
        load_data ods_order_info_inc ods_order_cargo_inc ods_transport_task_inc ods_order_org_bound_inc ods_user_info_inc ods_user_address_inc ods_base_complex_full ods_base_dic_full ods_base_region_info_full ods_base_organ_full ods_express_courier_full ods_express_courier_complex_full ods_employee_info_full ods_line_base_shift_full ods_line_base_info_full ods_truck_driver_full ods_truck_info_full ods_truck_model_full ods_truck_team_full
    ;;
esac

