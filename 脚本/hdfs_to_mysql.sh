#! /bin/bash

DATAX_HOME=/development/datax

#DataX导出路径不允许存在空文件，该函数作用为清理空文件
handle_export_path(){
  for i in `hadoop fs -ls -R $1 | awk '{print $8}'`; do
    hadoop fs -test -z $i
    if [[ $? -eq 0 ]]; then
      echo "$i文件大小为0，正在删除"
      hadoop fs -rm -r -f $i
    fi
  done
}

#数据导出
export_data() {
  datax_config=$1
  export_dir=$2
  handle_export_path $export_dir
  $DATAX_HOME/bin/datax.py -p"-Dexportdir=$export_dir" $datax_config
}

case $1 in
    "ads_trans_order_stats")
    export_data /development/datax/job/export/tms_report.ads_trans_order_stats.json /warehouse/tms/ads/ads_trans_order_stats
  ;;
  "ads_trans_stats")
    export_data /development/datax/job/export/tms_report.ads_trans_stats.json /warehouse/tms/ads/ads_trans_stats
  ;;
  "ads_trans_order_stats_td")
    export_data /development/datax/job/export/tms_report.ads_trans_order_stats_td.json /warehouse/tms/ads/ads_trans_order_stats_td
  ;;
  "ads_order_stats")
    export_data /development/datax/job/export/tms_report.ads_order_stats.json /warehouse/tms/ads/ads_order_stats
  ;;
  "ads_order_cargo_type_stats")
    export_data /development/datax/job/export/tms_report.ads_order_cargo_type_stats.json /warehouse/tms/ads/ads_order_cargo_type_stats
  ;;
  "ads_city_stats")
    export_data /development/datax/job/export/tms_report.ads_city_stats.json /warehouse/tms/ads/ads_city_stats
  ;;
  "ads_org_stats")
    export_data /development/datax/job/export/tms_report.ads_org_stats.json /warehouse/tms/ads/ads_org_stats
  ;;
  "ads_shift_stats")
    export_data /development/datax/job/export/tms_report.ads_shift_stats.json /warehouse/tms/ads/ads_shift_stats
  ;;
  "ads_line_stats")
    export_data /development/datax/job/export/tms_report.ads_line_stats.json /warehouse/tms/ads/ads_line_stats
  ;;
  "ads_driver_stats")
    export_data /development/datax/job/export/tms_report.ads_driver_stats.json /warehouse/tms/ads/ads_driver_stats
  ;;
  "ads_truck_stats")
    export_data /development/datax/job/export/tms_report.ads_truck_stats.json /warehouse/tms/ads/ads_truck_stats
  ;;
  "ads_express_stats")
    export_data /development/datax/job/export/tms_report.ads_express_stats.json /warehouse/tms/ads/ads_express_stats
  ;;
  "ads_express_province_stats")
    export_data /development/datax/job/export/tms_report.ads_express_province_stats.json /warehouse/tms/ads/ads_express_province_stats
  ;;
  "ads_express_city_stats")
    export_data /development/datax/job/export/tms_report.ads_express_city_stats.json /warehouse/tms/ads/ads_express_city_stats
  ;;
  "ads_express_org_stats")
    export_data /development/datax/job/export/tms_report.ads_express_org_stats.json /warehouse/tms/ads/ads_express_org_stats
  ;;
  "all")
    export_data /development/datax/job/export/tms_report.ads_trans_order_stats.json /warehouse/tms/ads/ads_trans_order_stats
    export_data /development/datax/job/export/tms_report.ads_trans_stats.json /warehouse/tms/ads/ads_trans_stats
    export_data /development/datax/job/export/tms_report.ads_trans_order_stats_td.json /warehouse/tms/ads/ads_trans_order_stats_td
    export_data /development/datax/job/export/tms_report.ads_order_stats.json /warehouse/tms/ads/ads_order_stats
    export_data /development/datax/job/export/tms_report.ads_order_cargo_type_stats.json /warehouse/tms/ads/ads_order_cargo_type_stats
    export_data /development/datax/job/export/tms_report.ads_city_stats.json /warehouse/tms/ads/ads_city_stats
    export_data /development/datax/job/export/tms_report.ads_org_stats.json /warehouse/tms/ads/ads_org_stats
    export_data /development/datax/job/export/tms_report.ads_shift_stats.json /warehouse/tms/ads/ads_shift_stats
    export_data /development/datax/job/export/tms_report.ads_line_stats.json /warehouse/tms/ads/ads_line_stats
    export_data /development/datax/job/export/tms_report.ads_driver_stats.json /warehouse/tms/ads/ads_driver_stats
    export_data /development/datax/job/export/tms_report.ads_truck_stats.json /warehouse/tms/ads/ads_truck_stats
    export_data /development/datax/job/export/tms_report.ads_express_stats.json /warehouse/tms/ads/ads_express_stats
    export_data /development/datax/job/export/tms_report.ads_express_province_stats.json /warehouse/tms/ads/ads_express_province_stats
    export_data /development/datax/job/export/tms_report.ads_express_city_stats.json /warehouse/tms/ads/ads_express_city_stats
    export_data /development/datax/job/export/tms_report.ads_express_org_stats.json /warehouse/tms/ads/ads_express_org_stats
  ;;
esac

