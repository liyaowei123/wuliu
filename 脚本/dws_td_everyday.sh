#! /bin/bash
#1、判断参数是否传入
if [ $# -lt 1 ]
then
	echo "必须传入all/表名..."
	exit
fi

#2、判断日期是否传入
[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)

dws_trans_dispatch_td_sql="
insert overwrite table dws_trans_dispatch_td
    partition (dt = '${datestr}')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from (select order_count,
             order_amount
      from dws_trans_dispatch_td
      where dt = date_add('${datestr}', -1)
      union
      select order_count,
             order_amount
      from dws_trans_dispatch_1d
      where dt = '${datestr}') all_data;
"

dws_trans_bound_finish_td_sql="
insert overwrite table dws_trans_bound_finish_td
    partition (dt = '${datestr}')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from (select order_count,
             order_amount
      from dws_trans_bound_finish_td
      where dt = date_add('${datestr}', -1)
      union
      select count(order_id)   order_count,
             sum(order_amount) order_amount
      from (select order_id,
                   max(amount) order_amount
            from dwd_trans_bound_finish_detail_inc
            where dt = '${datestr}'
            group by order_id) distinct_tb) all_data;
"
#3、匹配表名加载数据
case $1 in
"all")
	/development/hive/bin/hive -e "use tms;${dws_trans_bound_finish_td_sql}${dws_trans_dispatch_td_sql}"
;;
"dws_trans_bound_finish_td")
    /development/hive/bin/hive -e "use tms;${dws_trans_bound_finish_td_sql}"
;;
"dws_trans_dispatch_td")
    /development/hive/bin/hive -e "use tms;${dws_trans_dispatch_td_sql}"
;;
*)
	echo "表名输入错误..."
;;
esac

