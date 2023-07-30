use tms;

drop table ods_user_address_inc;

create external table if not exists ods_user_address_inc(
    op string comment '操作类型',
    after struct<
        address: string,
        is_deleted: string,
        create_time: string,
        user_id: bigint,
        phone: string,
        province_id: bigint,
        complex_id: bigint,
        id: bigint,
        district_id: bigint,
        is_default: int,
        city_id: bigint
    > comment '插入或修改前的数据',
    before struct<
        address: string,
        is_deleted: string,
        create_time: string,
        user_id: bigint,
        phone: string,
        province_id: bigint,
        complex_id: bigint,
        id: bigint,
        district_id: bigint,
        is_default: int,
        city_id: bigint
    > comment '插入或修改后的数据',
    ts bigint comment '时间戳'
) comment '用户地址表'
partitioned by (dt string)
row format SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/tms/ODS/ods_user_address_inc';

load data inpath '/origin_data/tms/user_address_inc/2023-07-07' overwrite into table ods_user_address_inc partition (dt = '2023-07-07');

select * from ods_user_address_inc;

show tables ;