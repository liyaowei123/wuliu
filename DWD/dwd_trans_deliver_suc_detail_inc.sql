drop table if exists dwd_trans_deliver_suc_detail_inc;
create external table dwd_trans_deliver_suc_detail_inc(
                                                          `id` bigint comment '运单明细ID',
                                                          `order_id` string COMMENT '运单ID',
                                                          `cargo_type` string COMMENT '货物类型ID',
                                                          `cargo_type_name` string COMMENT '货物类型名称',
                                                          `volumn_length` bigint COMMENT '长cm',
                                                          `volumn_width` bigint COMMENT '宽cm',
                                                          `volumn_height` bigint COMMENT '高cm',
                                                          `weight` decimal(16,2) COMMENT '重量 kg',
                                                          `deliver_suc_time` string COMMENT '派送成功时间',
                                                          `order_no` string COMMENT '运单号',
                                                          `status` string COMMENT '运单状态',
                                                          `status_name` string COMMENT '运单状态名称',
                                                          `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                                                          `collect_type_name` string COMMENT '取件类型名称',
                                                          `user_id` bigint COMMENT '用户ID',
                                                          `receiver_complex_id` bigint COMMENT '收件人小区id',
                                                          `receiver_province_id` string COMMENT '收件人省份id',
                                                          `receiver_city_id` string COMMENT '收件人城市id',
                                                          `receiver_district_id` string COMMENT '收件人区县id',
                                                          `receiver_name` string COMMENT '收件人姓名',
                                                          `sender_complex_id` bigint COMMENT '发件人小区id',
                                                          `sender_province_id` string COMMENT '发件人省份id',
                                                          `sender_city_id` string COMMENT '发件人城市id',
                                                          `sender_district_id` string COMMENT '发件人区县id',
                                                          `sender_name` string COMMENT '发件人姓名',
                                                          `payment_type` string COMMENT '支付方式',
                                                          `payment_type_name` string COMMENT '支付方式名称',
                                                          `cargo_num` bigint COMMENT '货物个数',
                                                          `amount` decimal(16,2) COMMENT '金额',
                                                          `estimate_arrive_time` string COMMENT '预计到达时间',
                                                          `distance` decimal(16,2) COMMENT '距离，单位：公里',
                                                          `ts` bigint COMMENT '时间戳'
) comment '物流域派送成功事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trans_deliver_suc_detail_inc'
    tblproperties('orc.compress' = 'snappy');



-------------------------------------首日数据加载-------------------------------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms.dwd_trans_deliver_suc_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       deliver_suc_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       ts,
       date_format(deliver_suc_time, 'yyyy-MM-dd') dt
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ts
      from ods_order_cargo_inc
      where dt = '2023-01-20'
        and after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*')                                  receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')                                    sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             date_format(from_utc_timestamp(
                                 cast(after.estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) deliver_suc_time
      from ods_order_info_inc
      where dt = '2023-01-20'
        and after.is_deleted = '0'
        and after.status <> '60010'
        and after.status <> '60020'
        and after.status <> '60030'
        and after.status <> '60040'
        and after.status <> '60050'
        and after.status <> '60060'
        and after.status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic_full
      where dt = '2023-01-20'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic_full
      where dt = '2023-01-20'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic_full
      where dt = '2023-01-20'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic_full
      where dt = '2023-01-20'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);



---------------------------------每日数据加载------------------------------------
with deliver_suc_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 deliver_suc_time,
                 ts
          from (select after.id,
                       after.order_no,
                       after.status,
                       after.collect_type,
                       after.user_id,
                       after.receiver_complex_id,
                       after.receiver_province_id,
                       after.receiver_city_id,
                       after.receiver_district_id,
                       concat(substr(after.receiver_name, 1, 1), '*')       receiver_name,
                       after.sender_complex_id,
                       after.sender_province_id,
                       after.sender_city_id,
                       after.sender_district_id,
                       concat(substr(after.sender_name, 1, 1), '*')         sender_name,
                       after.payment_type,
                       after.cargo_num,
                       after.amount,
                       date_format(from_utc_timestamp(
                                           cast(after.estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       after.distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(after.update_time, 1, 10), ' ',
                                                                    substr(after.update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') deliver_suc_time,
                       ts
                from ods_order_info_inc
                where dt = '2023-01-21'
                  and op = 'u'
                  and before.status = '60060'
                  and after.status = '60070'
                  and after.is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic_full
                where dt = '2023-01-21'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic_full
                where dt = '2023-01-21'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                cast(user_id as string),
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020' or
                status = '60030' or
                status = '60040' or
                status = '60050' or
                status = '60060')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select after.id,
                      after.order_id,
                      after.cargo_type,
                      after.volume_length,
                      after.volume_width,
                      after.volume_height,
                      after.weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(after.create_time, 1, 10), ' ',
                                                                   substr(after.create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                      ts
               from ods_order_cargo_inc
               where dt = '2023-01-21'
                 and op = 'c') cargo
                  join
              (select after.id,
                      after.order_no,
                      after.status,
                      after.collect_type,
                      after.user_id,
                      after.receiver_complex_id,
                      after.receiver_province_id,
                      after.receiver_city_id,
                      after.receiver_district_id,
                      concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
                      after.sender_complex_id,
                      after.sender_province_id,
                      after.sender_city_id,
                      after.sender_district_id,
                      concat(substr(after.sender_name, 1, 1), '*')   sender_name,
                      after.cargo_num,
                      after.amount,
                      date_format(from_utc_timestamp(
                                          cast(after.estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      after.distance
               from ods_order_info_inc
               where dt = '2023-01-21'
                 and op = 'c') info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic_full
               where dt = '2023-01-21'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic_full
               where dt = '2023-01-21'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic_full
               where dt = '2023-01-21'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms.dwd_trans_deliver_suc_detail_inc
partition(dt = '2023-01-21')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       deliver_suc_info.deliver_suc_time,
       order_info.order_no,
       deliver_suc_info.status,
       deliver_suc_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       deliver_suc_info.payment_type,
       deliver_suc_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       deliver_suc_info.ts
from deliver_suc_info
         join order_info
              on deliver_suc_info.id = order_info.order_id;
