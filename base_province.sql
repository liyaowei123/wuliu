create external table base_province (
    id bigint comment '省份id',
    name string comment '省份名称',
    region_id string comment '大区id',
    area_code string comment '行政区位码',
    iso_code string comment '国际编码',
    iso_3166_2 string comment 'ISO3166编码',
    create_time date comment '创建时间',
    update_time date comment '修改时间'
)
stored as textfile
location '/warehouse/tms/ODS/base_province/';

INSERT INTO  base_province  VALUES (1, '北京', '1', '110000', 'CN-11', 'CN-BJ', current_date(), current_date());
INSERT INTO  base_province  VALUES (2, '天津', '1', '120000', 'CN-12', 'CN-TJ', current_date(), current_date());
INSERT INTO  base_province  VALUES (3, '山西', '1', '140000', 'CN-14', 'CN-SX', current_date(), current_date());
INSERT INTO  base_province  VALUES (4, '内蒙古', '1', '150000', 'CN-15', 'CN-NM', current_date(), current_date());
INSERT INTO  base_province  VALUES (5, '河北', '1', '130000', 'CN-13', 'CN-HE', current_date(), current_date());
INSERT INTO  base_province  VALUES (6, '上海', '2', '310000', 'CN-31', 'CN-SH', current_date(), current_date());
INSERT INTO  base_province  VALUES (7, '江苏', '2', '320000', 'CN-32', 'CN-JS', current_date(), current_date());
INSERT INTO  base_province  VALUES (8, '浙江', '2', '330000', 'CN-33', 'CN-ZJ', current_date(), current_date());
INSERT INTO  base_province  VALUES (9, '安徽', '2', '340000', 'CN-34', 'CN-AH', current_date(), current_date());
INSERT INTO  base_province  VALUES (10, '福建', '2', '350000', 'CN-35', 'CN-FJ', current_date(), current_date());
INSERT INTO  base_province  VALUES (11, '江西', '2', '360000', 'CN-36', 'CN-JX', current_date(), current_date());
INSERT INTO  base_province  VALUES (12, '山东', '2', '370000', 'CN-37', 'CN-SD', current_date(), current_date());
INSERT INTO  base_province  VALUES (14, '台湾', '2', '710000', 'CN-71', 'CN-TW', current_date(), current_date());
INSERT INTO  base_province  VALUES (15, '黑龙江', '3', '230000', 'CN-23', 'CN-HL', current_date(), current_date());
INSERT INTO  base_province  VALUES (16, '吉林', '3', '220000', 'CN-22', 'CN-JL', current_date(), current_date());
INSERT INTO  base_province  VALUES (17, '辽宁', '3', '210000', 'CN-21', 'CN-LN', current_date(), current_date());
INSERT INTO  base_province  VALUES (18, '陕西', '7', '610000', 'CN-61', 'CN-SN', current_date(), current_date());
INSERT INTO  base_province  VALUES (19, '甘肃', '7', '620000', 'CN-62', 'CN-GS', current_date(), current_date());
INSERT INTO  base_province  VALUES (20, '青海', '7', '630000', 'CN-63', 'CN-QH', current_date(), current_date());
INSERT INTO  base_province  VALUES (21, '宁夏', '7', '640000', 'CN-64', 'CN-NX', current_date(), current_date());
INSERT INTO  base_province  VALUES (22, '新疆', '7', '650000', 'CN-65', 'CN-XJ', current_date(), current_date());
INSERT INTO  base_province  VALUES (23, '河南', '4', '410000', 'CN-41', 'CN-HA', current_date(), current_date());
INSERT INTO  base_province  VALUES (24, '湖北', '4', '420000', 'CN-42', 'CN-HB', current_date(), current_date());
INSERT INTO  base_province  VALUES (25, '湖南', '4', '430000', 'CN-43', 'CN-HN', current_date(), current_date());
INSERT INTO  base_province  VALUES (26, '广东', '5', '440000', 'CN-44', 'CN-GD', current_date(), current_date());
INSERT INTO  base_province  VALUES (27, '广西', '5', '450000', 'CN-45', 'CN-GX', current_date(), current_date());
INSERT INTO  base_province  VALUES (28, '海南', '5', '460000', 'CN-46', 'CN-HI', current_date(), current_date());
INSERT INTO  base_province  VALUES (29, '香港', '5', '810000', 'CN-91', 'CN-HK', current_date(), current_date());
INSERT INTO  base_province  VALUES (30, '澳门', '5', '820000', 'CN-92', 'CN-MO', current_date(), current_date());
INSERT INTO  base_province  VALUES (31, '四川', '6', '510000', 'CN-51', 'CN-SC', current_date(), current_date());
INSERT INTO  base_province  VALUES (32, '贵州', '6', '520000', 'CN-52', 'CN-GZ', current_date(), current_date());
INSERT INTO  base_province  VALUES (33, '云南', '6', '530000', 'CN-53', 'CN-YN', current_date(), current_date());
INSERT INTO  base_province  VALUES (13, '重庆', '6', '500000', 'CN-50', 'CN-CQ', current_date(), current_date());
INSERT INTO  base_province  VALUES (34, '西藏', '6', '540000', 'CN-54', 'CN-X', current_date(), current_date());