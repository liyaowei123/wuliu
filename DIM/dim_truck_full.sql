drop table if exists dim_truck_full;
create external table dim_truck_full(
                                        `id` bigint COMMENT '卡车ID',
                                        `team_id` bigint COMMENT '所属车队ID',
                                        `team_name` string COMMENT '所属车队名称',
                                        `team_no` string COMMENT '车队编号',
                                        `org_id` bigint COMMENT '所属机构',
                                        `org_name` string COMMENT '所属机构名称',
                                        `manager_emp_id` bigint COMMENT '负责人',
                                        `truck_no` string COMMENT '车牌号码',
                                        `truck_model_id` string COMMENT '型号',
                                        `truck_model_name` string COMMENT '型号名称',
                                        `truck_model_type` string COMMENT '型号类型',
                                        `truck_model_type_name` string COMMENT '型号类型名称',
                                        `truck_model_no` string COMMENT '型号编码',
                                        `truck_brand` string COMMENT '品牌',
                                        `truck_brand_name` string COMMENT '品牌名称',
                                        `truck_weight` decimal(16,2) COMMENT '整车重量（吨）',
                                        `load_weight` decimal(16,2) COMMENT '额定载重（吨）',
                                        `total_weight` decimal(16,2) COMMENT '总质量（吨）',
                                        `eev` string COMMENT '排放标准',
                                        `boxcar_len` decimal(16,2) COMMENT '货箱长（m）',
                                        `boxcar_wd` decimal(16,2) COMMENT '货箱宽（m）',
                                        `boxcar_hg` decimal(16,2) COMMENT '货箱高（m）',
                                        `max_speed` bigint COMMENT '最高时速（千米/时）',
                                        `oil_vol` bigint COMMENT '油箱容积（升）',
                                        `device_gps_id` string COMMENT 'GPS设备ID',
                                        `engine_no` string COMMENT '发动机编码',
                                        `license_registration_date` string COMMENT '注册时间',
                                        `license_last_check_date` string COMMENT '最后年检日期',
                                        `license_expire_date` string COMMENT '失效日期',
                                        `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '卡车维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/DIM/dim_truck_full'
    tblproperties('orc.compress'='snappy');


insert overwrite table tms.dim_truck_full
    partition (dt = '2023-01-20')
select truck_info.id,
       team_id,
       team_info.name     team_name,
       team_no,
       org_id,
       org_name,
       manager_emp_id,
       truck_no,
       truck_model_id,
       model_name         truck_model_name,
       model_type         truck_model_type,
       dic_for_type.name  truck_model_type_name,
       model_no           truck_model_no,
       brand              truck_brand,
       dic_for_brand.name truck_brand_name,
       truck_weight,
       load_weight,
       total_weight,
       eev,
       boxcar_len,
       boxcar_wd,
       boxcar_hg,
       max_speed,
       oil_vol,
       device_gps_id,
       engine_no,
       license_registration_date,
       license_last_check_date,
       license_expire_date,
       is_enabled
from (select id,
             team_id,

             md5(truck_no) truck_no,
             truck_model_id,

             device_gps_id,
             engine_no,
             license_registration_date,
             license_last_check_date,
             license_expire_date,
             is_enabled
      from ods_truck_info_full
      where dt = '2023-01-20'
        and is_deleted = '0') truck_info
         join
     (select id,
             name,
             team_no,
             org_id,

             manager_emp_id
      from ods_truck_team_full
      where dt = '2023-01-20'
        and is_deleted = '0') team_info
     on truck_info.team_id = team_info.id
         join
     (select id,
             model_name,
             model_type,

             model_no,
             brand,

             truck_weight,
             load_weight,
             total_weight,
             eev,
             boxcar_len,
             boxcar_wd,
             boxcar_hg,
             max_speed,
             oil_vol
      from ods_truck_model_full
      where dt = '2023-01-20'
        and is_deleted = '0') model_info
     on truck_info.truck_model_id = model_info.id
         join
     (select id,
             org_name
      from ods_base_organ_full
      where dt = '2023-01-20'
        and is_deleted = '0'
     ) organ_info
     on org_id = organ_info.id
         join
     (select id,
             name
      from ods_base_dic_full
      where dt = '2023-01-20'
        and is_deleted = '0') dic_for_type
     on model_info.model_type = dic_for_type.id
         join
     (select id,
             name
      from ods_base_dic_full
      where dt = '2023-01-20'
        and is_deleted = '0') dic_for_brand
     on model_info.brand = dic_for_brand.id;
