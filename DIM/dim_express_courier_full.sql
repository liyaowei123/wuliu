drop table if exists dim_express_courier_full;
create external table dim_express_courier_full(
                                                  `id` bigint COMMENT '快递员ID',
                                                  `emp_id` bigint COMMENT '员工ID',
                                                  `org_id` bigint COMMENT '所属机构ID',
                                                  `org_name` string COMMENT '机构名称',
                                                  `working_phone` string COMMENT '工作电话',
                                                  `express_type` string COMMENT '快递员类型（收货；发货）',
                                                  `express_type_name` string COMMENT '快递员类型名称'
) comment '快递员维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/DIM/dim_express_courier_full'
    tblproperties('orc.compress'='snappy');

insert overwrite table tms.dim_express_courier_full
    partition (dt = '2023-01-20')
select express_cor_info.id,
       emp_id,
       org_id,
       org_name,
       working_phone,
       express_type,
       dic_info.name express_type_name
from (select id,
             emp_id,
             org_id,
             md5(working_phone) working_phone,
             express_type
      from ods_express_courier_full
      where dt = '2023-01-20'
        and is_deleted = '0') express_cor_info
         join (
    select id,
           org_name
    from ods_base_organ_full
    where dt = '2023-01-20'
      and is_deleted = '0'
) organ_info
              on express_cor_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_base_dic_full
    where dt = '2023-01-20'
      and is_deleted = '0'
) dic_info
              on express_type = dic_info.id;


