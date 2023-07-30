#!/bin/bash

DATAX_HOME=/development/datax
DATAX_DATA=/development/datax/job

#清理脏数据
handle_targetdir() {
  hadoop fs -rm -r $1 >/dev/null 2>&1
  hadoop fs -mkdir -p $1
}

#数据同步
import_data() {
  local datax_config=$1
  local target_dir=$2

  handle_targetdir "$target_dir"
  echo "正在处理$1"
  python $DATAX_HOME/bin/datax.py -p"-Dtargetdir=$target_dir" $datax_config >/tmp/datax_run.log 2>&1
  if [ $? -ne 0 ]
  then
    echo "处理失败, 日志如下:"
    cat /tmp/datax_run.log 
  fi
}

#接收表名变量
tab=$1
# 如果传入日期则do_date等于传入的日期，否则等于前一天日期
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=$(date -d "-1 day" +%F)
fi


case ${tab} in
base_complex | base_dic | base_region_info | base_organ | express_courier | express_courier_complex | employee_info | line_base_shift | line_base_info | truck_driver | truck_info | truck_model | truck_team)
  import_data $DATAX_DATA/import/tms.${tab}.json /origin_data/tms/${tab}_full/$do_date
  ;;
"all")
  for tmp in base_complex base_dic base_region_info base_organ express_courier express_courier_complex employee_info line_base_shift line_base_info truck_driver truck_info truck_model truck_team
  do
    import_data $DATAX_DATA/import/tms.${tmp}.json /origin_data/tms/${tmp}_full/$do_date
  done
  ;;
esac

