drop table if exists ads_line_stats;
create external table ads_line_stats(
                                        `dt` string COMMENT '统计日期',
                                        `recent_days` tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
                                        `line_id` bigint COMMENT '线路ID',
                                        `line_name` string COMMENT '线路名称',
                                        `trans_finish_count` bigint COMMENT '完成运输次数',
                                        `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
                                        `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
                                        `trans_finish_order_count` bigint COMMENT '运输完成运单数'
) comment '线路分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_line_stats';

--数据加载--
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
select '2023-01-20'                  dt,
       recent_days,
       line_id,
       line_name,
       sum(trans_finish_count)       trans_finish_count,
       sum(trans_finish_distance)    trans_finish_distance,
       sum(trans_finish_dur_sec)     trans_finish_dur_sec,
       sum(trans_finish_order_count) trans_finish_order_count
from dws_trans_shift_trans_finish_nd
where dt = '2023-01-20'
group by line_id,
         line_name,
         recent_days;
