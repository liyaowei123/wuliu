drop table if exists ads_truck_stats;
create external table ads_truck_stats(
  `dt` string COMMENT '统计日期',
  `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `truck_model_type` string COMMENT '卡车类别编码',
  `truck_model_type_name` string COMMENT '卡车类别名称',
  `trans_finish_count` bigint COMMENT '完成运输次数',
  `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
  `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
  `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
  `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒'
) comment '卡车分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_truck_stats';

--数据加载--
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
select '2023-01-20'                                         dt,
       recent_days,
       truck_model_type,
       truck_model_type_name,
       sum(trans_finish_count)                              trans_finish_count,
       sum(trans_finish_distance)                           trans_finish_distance,
       sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
from dws_trans_shift_trans_finish_nd
where dt = '2023-01-20'
group by truck_model_type,
         truck_model_type_name,
         recent_days;
