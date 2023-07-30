use tms;

drop table ods_order_info_inc;

create external table if not exists ods_order_info_inc(
    op string comment '操作类型',
    after struct<order_no: string,
                 sender_phone: string,
                 distance: bigint,
                 receiver_city_id: string,
                 sender_name: string,
                 receiver_province_id: string,
                 receiver_district_id: string,
                 update_time: string,
                 is_deleted: string,
                 collect_type: string,
                 receiver_complex_id: bigint,
                 cargo_num: bigint,
                 receiver_name: string,
                 id: bigint,
                 receive_location: string,
                 amount: bigint,
                 sender_complex_id: bigint,
                 create_time: string,
                 sender_district_id: string,
                 sender_address: string,
                 payment_type: string,
                 sender_city_id: string,
                 user_id: string,
                 sender_province_id: string,
                 receiver_address: string,
                 receiver_phone: string,
                 estimate_arrive_time: string,
                 status: string
    > comment '插入或修改前的数据',
    before struct<order_no: string,
                  sender_phone: string,
                  distance: bigint,
                  receiver_city_id: string,
                  sender_name: string,
                  receiver_province_id: string,
                  receiver_district_id: string,
                  update_time: string,
                  is_deleted: string,
                  collect_type: string,
                  receiver_complex_id: bigint,
                  cargo_num: bigint,
                  receiver_name: string,
                  id: bigint,
                  receive_location: string,
                  amount: bigint,
                  sender_complex_id: bigint,
                  create_time: string,
                  sender_district_id: string,
                  sender_address: string,
                  payment_type: string,
                  sender_city_id: string,
                  user_id: string,
                  sender_province_id: string,
                  receiver_address: string,
                  receiver_phone: string,
                  estimate_arrive_time: string,
                  status: string
    > comment '插入或修改后的数据',
    ts bigint comment '时间戳'
) comment '运单表'
partitioned by (dt string)
row format SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
location '/warehouse/tms/ODS/ods_order_info_inc';

load data inpath '/origin_data/tms/order_info_inc/2023-07-07' overwrite into table ods_order_info_inc partition (dt = '2023-07-07');

drop table ods_order_info_inc;

select * from ods_order_cargo_inc;

desc ods_order_info_inc;
