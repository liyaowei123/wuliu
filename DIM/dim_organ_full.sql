drop table if exists dim_organ_full;
create external table dim_organ_full(
                                        `id` bigint COMMENT '机构ID',
                                        `org_name` string COMMENT '机构名称',
                                        `org_level` bigint COMMENT '机构等级（1为转运中心，2为转运站）',
                                        `region_id` bigint COMMENT '地区ID，1级机构为city ,2级机构为district',
                                        `region_name` string COMMENT '地区名称',
                                        `region_code` string COMMENT '地区编码（行政级别）',
                                        `org_parent_id` bigint COMMENT '父级机构ID',
                                        `org_parent_name` string COMMENT '父级机构名称'
) comment '机构维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/DIM/dim_organ_full'
    tblproperties('orc.compress'='snappy');

insert overwrite table tms.dim_organ_full
    partition (dt = '2023-01-20')
select organ_info.id,
       organ_info.org_name,
       org_level,
       region_id,
       region_info.name        region_name,
       region_info.dict_code   region_code,
       org_parent_id,
       org_for_parent.org_name org_parent_name
from (select id,
             org_name,
             org_level,
             region_id,
             org_parent_id
      from ods_base_organ_full
      where dt = '2023-01-20'
        and is_deleted = '0') organ_info
         left join (
    select id,
           name,
           dict_code
    from ods_base_region_info_full
    where dt = '2023-01-20'
      and is_deleted = '0'
) region_info
                   on organ_info.region_id = region_info.id
         left join (
    select id,
           org_name
    from ods_base_organ_full
    where dt = '2023-01-20'
      and is_deleted = '0'
) org_for_parent
                   on organ_info.org_parent_id = org_for_parent.id;

