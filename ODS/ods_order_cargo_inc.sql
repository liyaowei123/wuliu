use tms;

drop table ods_order_cargo_inc;

create external table if not exists ods_order_cargo_inc(
    op string comment '操作类型',
    after struct<
        is_deleted: string,
        volume_length: int,
        create_time: string,
        volume_height: int,
        weight: decimal,
        cargo_type: string,
        id: bigint,
        order_id: string,
        volume_width: int
    > comment '插入或修改后的数据',
    before struct<is_deleted: string,
                  volume_length: int,
                  create_time: string,
                  volume_height: int,
                  weight: decimal,
                  cargo_type: string,
                  id: bigint,
                  order_id: string,
                  volume_width: int
    > comment '插入或修改前的数据',
    ts bigint comment '时间戳'
) comment '运单明细表'
partitioned by (dt string)
row format SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
location '/warehouse/tms/ODS/ods_order_cargo_inc';

load data inpath '/origin_data/tms/order_cargo_inc/2023-01-10' overwrite into table ods_order_cargo_inc partition (dt = '2023-01-10');

select *
from ods_order_cargo_inc;

show partitions ods_order_cargo_inc;
alter table ods_order_cargo_inc drop partition (dt = '2023-07-15');
