drop table if exists ads_trans_stats;
create external table ads_trans_stats(
  `dt` string COMMENT '统计日期',
  `recent_days` tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `trans_finish_count` bigint COMMENT '完成运输次数',
  `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
  `trans_finish_dur_sec` bigint COMMENT ' 完成运输时长，单位：秒'
) comment '运输综合统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_trans_stats';

--数据加载--
insert overwrite table ads_trans_stats
select dt,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec
from ads_trans_stats
union
select '2023-01-20'               dt,
       1                          recent_days,
       sum(trans_finish_count)    trans_finish_count,
       sum(trans_finish_distance) trans_finish_distance,
       sum(trans_finish_dur_sec)  trans_finish_dur_sec
from dws_trans_org_truck_model_type_trans_finish_1d
where dt = '2023-01-20'
union
select '2023-01-20'               dt,
       recent_days,
       sum(trans_finish_count)    trans_finish_count,
       sum(trans_finish_distance) trans_finish_distance,
       sum(trans_finish_dur_sec)  trans_finish_dur_sec
from dws_trans_shift_trans_finish_nd
where dt = '2023-01-20'
group by recent_days;
