drop table if exists dim_truck_driver_full;
create external table dim_truck_driver_full(
                                               `id` bigint COMMENT '司机信息ID',
                                               `emp_id` bigint COMMENT '员工ID',
                                               `org_id` bigint COMMENT '所属机构ID',
                                               `org_name` string COMMENT '所属机构名称',
                                               `team_id` bigint COMMENT '所属车队ID',
                                               `tream_name` string COMMENT '所属车队名称',
                                               `license_type` string COMMENT '准驾车型',
                                               `init_license_date` string COMMENT '初次领证日期',
                                               `expire_date` string COMMENT '有效截止日期',
                                               `license_no` string COMMENT '驾驶证号',
                                               `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '司机维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/DIM/dim_truck_driver_full'
    tblproperties('orc.compress'='snappy');


insert overwrite table tms.dim_truck_driver_full
    partition (dt = '2023-01-20')
select driver_info.id,
       emp_id,
       org_id,
       organ_info.org_name,
       team_id,
       team_info.name team_name,
       license_type,
       init_license_date,
       expire_date,
       license_no,
       is_enabled
from (select id,
             emp_id,
             org_id,
             team_id,
             license_type,
             init_license_date,
             expire_date,
             license_no,
             is_enabled
      from ods_truck_driver_full
      where dt = '2023-01-20'
        and is_deleted = '0') driver_info
         join (
    select id,
           org_name
    from ods_base_organ_full
    where dt = '2023-01-20'
      and is_deleted = '0'
) organ_info
              on driver_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_truck_team_full
    where dt = '2023-01-20'
      and is_deleted = '0'
) team_info
              on driver_info.team_id = team_info.id;
