drop table if exists ods_employee_info_full;
create external table ods_employee_info_full(
                                                id  bigint COMMENT '员工ID',
                                                username  string COMMENT '用户名',
                                                password  string COMMENT '密码',
                                                real_name  string COMMENT '真实姓名',
                                                id_card  string COMMENT '身份证号',
                                                phone  string COMMENT '手机号',
                                                birthday  string COMMENT '生日',
                                                gender  string COMMENT '性别',
                                                address  string COMMENT '地址',
                                                employment_date  string COMMENT '入职日期',
                                                graduation_date  string COMMENT '离职日期',
                                                education  string COMMENT '学历',
                                                position_type  string COMMENT '岗位类别',
                                                create_time  string COMMENT '创建时间',
                                                update_time  string COMMENT '更新时间',
                                                is_deleted  string COMMENT '删除标记（0:不可用 1:可用）'
) comment '员工表'
    partitioned by ( dt  string comment '统计日期')
    row format delimited fields terminated by '\t'
        null defined as ' '
    location '/warehouse/tms/ODS/ods_employee_info_full';
