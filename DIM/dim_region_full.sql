drop table if exists dim_region_full;
create external table dim_region_full(
                                         `id` bigint COMMENT '地区ID',
                                         `parent_id` bigint COMMENT '上级地区ID',
                                         `name` string COMMENT '地区名称',
                                         `dict_code` string COMMENT '编码（行政级别）',
                                         `short_name` string COMMENT '简称'
) comment '地区维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/DIM/dim_region_full'
    tblproperties('orc.compress'='snappy');


insert overwrite table dim_region_full
    partition (dt = '2023-01-20')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info_full
where dt = '2023-01-20'
  and is_deleted = '0';