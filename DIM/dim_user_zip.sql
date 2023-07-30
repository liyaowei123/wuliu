drop table if exists dim_user_zip;
create external table dim_user_zip(
                                      `id` bigint COMMENT '用户地址信息ID',
                                      `login_name` string COMMENT '用户名称',
                                      `nick_name` string COMMENT '用户昵称',
                                      `passwd` string COMMENT '用户密码',
                                      `real_name` string COMMENT '用户姓名',
                                      `phone_num` string COMMENT '手机号',
                                      `email` string COMMENT '邮箱',
                                      `user_level` string COMMENT '用户级别',
                                      `birthday` string COMMENT '用户生日',
                                      `gender` string COMMENT '性别 M男,F女',
                                      `start_date` string COMMENT '起始日期',
                                      `end_date` string COMMENT '结束日期'
) comment '用户拉链表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/DIM/dim_user_zip'
    tblproperties('orc.compress'='snappy');

--------------首日数据加载-------------
insert overwrite table dim_user_zip
    partition (dt = '9999-12-31')
select after.id,
       after.login_name,
       after.nick_name,
       md5(after.passwd)                                                                                    passwd,
       md5(after.real_name)                                                                                 realname,
       md5(if(after.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
              after.phone_num, null))                                                                       phone_num,
       md5(if(after.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', after.email, null)) email,
       after.user_level,
       date_add('1970-01-01', cast(after.birthday as int))                                                  birthday,
       after.gender,
       date_format(from_utc_timestamp(
                           cast(after.create_time as bigint), 'UTC'),
                   'yyyy-MM-dd')                                                                            start_date,
       '9999-12-31'                                                                                         end_date
from ods_user_info_inc
where dt = '2023-01-20'
  and after.is_deleted = '0';


-------每日数据加载--------
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
insert overwrite table dim_user_zip
    partition (dt)
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       start_date,
       if(rk = 1, end_date, date_add('2023-01-21', -1)) end_date,
       if(rk = 1, end_date, date_add('2023-01-21', -1)) dt
from (select id,
             login_name,
             nick_name,
             passwd,
             real_name,
             phone_num,
             email,
             user_level,
             birthday,
             gender,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
      from (select id,
                   login_name,
                   nick_name,
                   passwd,
                   real_name,
                   phone_num,
                   email,
                   user_level,
                   birthday,
                   gender,
                   start_date,
                   end_date
            from dim_user_zip
            where dt = '9999-12-31'
            union
            select id,
                   login_name,
                   nick_name,
                   md5(passwd)                                                                              passwd,
                   md5(real_name)                                                                           realname,
                   md5(if(phone_num regexp
                          '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
                          phone_num, null))                                                                 phone_num,
                   md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$', email, null)) email,
                   user_level,
                   cast(date_add('1970-01-01', cast(birthday as int)) as string)                            birthday,
                   gender,
                   '2023-01-21'                                                                             start_date,
                   '9999-12-31'                                                                             end_date
            from (select after.id,
                         after.login_name,
                         after.nick_name,
                         after.passwd,
                         after.real_name,
                         after.phone_num,
                         after.email,
                         after.user_level,
                         after.birthday,
                         after.gender,
                         row_number() over (partition by after.id order by ts desc) rn
                  from ods_user_info_inc
                  where dt = '2023-01-21'
                    and after.is_deleted = '0'
                 ) inc
            where rn = 1) full_info) final_info;


