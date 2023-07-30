use tms;

drop table ods_user_info_inc;

create external table if not exists ods_user_info_inc(
    op string comment '操作类型',
    after struct<
        birthday: bigint,
        gender: string,
        create_time: bigint,
        real_name: string,
        login_name: string,
        is_deleted: string,
        passwd: string,
        nick_name: string,
        user_level: string,
        phone_num: string,
        id: bigint,
        email: string
    > comment '插入或修改前的数据',
    before struct<
        birthday: bigint,
        gender: string,
        create_time: bigint,
        real_name: string,
        login_name: string,
        is_deleted: string,
        passwd: string,
        nick_name: string,
        user_level: string,
        phone_num: string,
        id: bigint,
        email: string
    > comment '插入或修改后的数据',
    ts bigint comment '时间戳'
) comment '用户信息表'
    partitioned by (dt string)
    row format SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/warehouse/tms/ODS/ods_user_info_inc';

load data inpath '/origin_data/tms/user_info_inc/2023-07-07' overwrite into table ods_user_info_inc partition (dt = '2023-07-07');

select *
from ods_user_info_inc;