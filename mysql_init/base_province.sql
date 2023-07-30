USE tms;

SELECT * FROM base_province;
USE tms;
SELECT * FROM base_province;
TRUNCATE TABLE base_province;
INSERT INTO `base_province` VALUES (1, '北京', '1', '110000', 'CN-11', 'CN-BJ', NOW(),NOW());
INSERT INTO `base_province` VALUES (2, '天津', '1', '120000', 'CN-12', 'CN-TJ', NOW(),NOW());
INSERT INTO `base_province` VALUES (3, '山西', '1', '140000', 'CN-14', 'CN-SX', NOW(),NOW());
INSERT INTO `base_province` VALUES (4, '内蒙古', '1', '150000', 'CN-15', 'CN-NM', NOW(),NOW());
INSERT INTO `base_province` VALUES (5, '河北', '1', '130000', 'CN-13', 'CN-HE', NOW(),NOW());
INSERT INTO `base_province` VALUES (6, '上海', '2', '310000', 'CN-31', 'CN-SH', NOW(),NOW());
INSERT INTO `base_province` VALUES (7, '江苏', '2', '320000', 'CN-32', 'CN-JS', NOW(),NOW());
INSERT INTO `base_province` VALUES (8, '浙江', '2', '330000', 'CN-33', 'CN-ZJ', NOW(),NOW());
INSERT INTO `base_province` VALUES (9, '安徽', '2', '340000', 'CN-34', 'CN-AH', NOW(),NOW());
INSERT INTO `base_province` VALUES (10, '福建', '2', '350000', 'CN-35', 'CN-FJ', NOW(),NOW());
INSERT INTO `base_province` VALUES (11, '江西', '2', '360000', 'CN-36', 'CN-JX', NOW(),NOW());
INSERT INTO `base_province` VALUES (12, '山东', '2', '370000', 'CN-37', 'CN-SD', NOW(),NOW());
INSERT INTO `base_province` VALUES (13, '重庆', '6', '500000', 'CN-50', 'CN-CQ', NOW(),NOW());
INSERT INTO `base_province` VALUES (14, '台湾', '2', '710000', 'CN-71', 'CN-TW', NOW(),NOW());
INSERT INTO `base_province` VALUES (15, '黑龙江', '3', '230000', 'CN-23', 'CN-HL', NOW(),NOW());
INSERT INTO `base_province` VALUES (16, '吉林', '3', '220000', 'CN-22', 'CN-JL', NOW(),NOW());
INSERT INTO `base_province` VALUES (17, '辽宁', '3', '210000', 'CN-21', 'CN-LN', NOW(),NOW());
INSERT INTO `base_province` VALUES (18, '陕西', '7', '610000', 'CN-61', 'CN-SN', NOW(),NOW());
INSERT INTO `base_province` VALUES (19, '甘肃', '7', '620000', 'CN-62', 'CN-GS', NOW(),NOW());
INSERT INTO `base_province` VALUES (20, '青海', '7', '630000', 'CN-63', 'CN-QH', NOW(),NOW());
INSERT INTO `base_province` VALUES (21, '宁夏', '7', '640000', 'CN-64', 'CN-NX', NOW(),NOW());
INSERT INTO `base_province` VALUES (22, '新疆', '7', '650000', 'CN-65', 'CN-XJ', NOW(),NOW());
INSERT INTO `base_province` VALUES (23, '河南', '4', '410000', 'CN-41', 'CN-HA', NOW(),NOW());
INSERT INTO `base_province` VALUES (24, '湖北', '4', '420000', 'CN-42', 'CN-HB', NOW(),NOW());
INSERT INTO `base_province` VALUES (25, '湖南', '4', '430000', 'CN-43', 'CN-HN', NOW(),NOW());
INSERT INTO `base_province` VALUES (26, '广东', '5', '440000', 'CN-44', 'CN-GD', NOW(),NOW());
INSERT INTO `base_province` VALUES (27, '广西', '5', '450000', 'CN-45', 'CN-GX', NOW(),NOW());
INSERT INTO `base_province` VALUES (28, '海南', '5', '460000', 'CN-46', 'CN-HI', NOW(),NOW());
INSERT INTO `base_province` VALUES (29, '香港', '5', '810000', 'CN-91', 'CN-HK', NOW(),NOW());
INSERT INTO `base_province` VALUES (30, '澳门', '5', '820000', 'CN-92', 'CN-MO', NOW(),NOW());
INSERT INTO `base_province` VALUES (31, '四川', '6', '510000', 'CN-51', 'CN-SC', NOW(),NOW());
INSERT INTO `base_province` VALUES (32, '贵州', '6', '520000', 'CN-52', 'CN-GZ', NOW(),NOW());
INSERT INTO `base_province` VALUES (33, '云南', '6', '530000', 'CN-53', 'CN-YN', NOW(),NOW());
INSERT INTO `base_province` VALUES (34, '西藏', '6', '540000', 'CN-54', 'CN-XZ', NOW(),NOW());


DROP TABLE IF EXISTS `base_province`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `base_province` (
  `id` BIGINT(20) DEFAULT NULL COMMENT 'id',
  `name` VARCHAR(20) DEFAULT NULL COMMENT '省名称',
  `region_id` VARCHAR(20) DEFAULT NULL COMMENT '大区id',
  `area_code` VARCHAR(20) DEFAULT NULL COMMENT '行政区位码',
  `iso_code` VARCHAR(20) DEFAULT NULL COMMENT '国际编码',
  `iso_3166_2` VARCHAR(20) DEFAULT NULL COMMENT 'ISO3166编码',
  `create_time` DATE DEFAULT NULL,
  `operator_time` DATE DEFAULT NULL
) ENGINE=INNODB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

INSERT INTO `base_province` VALUES (1, '北京', '1', '110000', 'CN-11', 'CN-BJ', NOW(), NOW());
INSERT INTO `base_province` VALUES (2, '天津', '1', '120000', 'CN-12', 'CN-TJ', NOW(), NOW());
INSERT INTO `base_province` VALUES (3, '山西', '1', '140000', 'CN-14', 'CN-SX', NOW(), NOW());
INSERT INTO `base_province` VALUES (4, '内蒙古', '1', '150000', 'CN-15', 'CN-NM', NOW(), NOW());
INSERT INTO `base_province` VALUES (5, '河北', '1', '130000', 'CN-13', 'CN-HE', NOW(), NOW());
INSERT INTO `base_province` VALUES (6, '上海', '2', '310000', 'CN-31', 'CN-SH', NOW(), NOW());
INSERT INTO `base_province` VALUES (7, '江苏', '2', '320000', 'CN-32', 'CN-JS', NOW(), NOW());
INSERT INTO `base_province` VALUES (8, '浙江', '2', '330000', 'CN-33', 'CN-ZJ', NOW(), NOW());
INSERT INTO `base_province` VALUES (9, '安徽', '2', '340000', 'CN-34', 'CN-AH', NOW(), NOW());
INSERT INTO `base_province` VALUES (10, '福建', '2', '350000', 'CN-35', 'CN-FJ', NOW(), NOW());
INSERT INTO `base_province` VALUES (11, '江西', '2', '360000', 'CN-36', 'CN-JX', NOW(), NOW());
INSERT INTO `base_province` VALUES (12, '山东', '2', '370000', 'CN-37', 'CN-SD', NOW(), NOW());
INSERT INTO `base_province` VALUES (14, '台湾', '2', '710000', 'CN-71', 'CN-TW', NOW(), NOW());
INSERT INTO `base_province` VALUES (15, '黑龙江', '3', '230000', 'CN-23', 'CN-HL', NOW(), NOW());
INSERT INTO `base_province` VALUES (16, '吉林', '3', '220000', 'CN-22', 'CN-JL', NOW(), NOW());
INSERT INTO `base_province` VALUES (17, '辽宁', '3', '210000', 'CN-21', 'CN-LN', NOW(), NOW());
INSERT INTO `base_province` VALUES (18, '陕西', '7', '610000', 'CN-61', 'CN-SN', NOW(), NOW());
INSERT INTO `base_province` VALUES (19, '甘肃', '7', '620000', 'CN-62', 'CN-GS', NOW(), NOW());
INSERT INTO `base_province` VALUES (20, '青海', '7', '630000', 'CN-63', 'CN-QH', NOW(), NOW());
INSERT INTO `base_province` VALUES (21, '宁夏', '7', '640000', 'CN-64', 'CN-NX', NOW(), NOW());
INSERT INTO `base_province` VALUES (22, '新疆', '7', '650000', 'CN-65', 'CN-XJ', NOW(), NOW());
INSERT INTO `base_province` VALUES (23, '河南', '4', '410000', 'CN-41', 'CN-HA', NOW(), NOW());
INSERT INTO `base_province` VALUES (24, '湖北', '4', '420000', 'CN-42', 'CN-HB', NOW(), NOW());
INSERT INTO `base_province` VALUES (25, '湖南', '4', '430000', 'CN-43', 'CN-HN', NOW(), NOW());
INSERT INTO `base_province` VALUES (26, '广东', '5', '440000', 'CN-44', 'CN-GD', NOW(), NOW());
INSERT INTO `base_province` VALUES (27, '广西', '5', '450000', 'CN-45', 'CN-GX', NOW(), NOW());
INSERT INTO `base_province` VALUES (28, '海南', '5', '460000', 'CN-46', 'CN-HI', NOW(), NOW());
INSERT INTO `base_province` VALUES (29, '香港', '5', '810000', 'CN-91', 'CN-HK', NOW(), NOW());
INSERT INTO `base_province` VALUES (30, '澳门', '5', '820000', 'CN-92', 'CN-MO', NOW(), NOW());
INSERT INTO `base_province` VALUES (31, '四川', '6', '510000', 'CN-51', 'CN-SC', NOW(), NOW());
INSERT INTO `base_province` VALUES (32, '贵州', '6', '520000', 'CN-52', 'CN-GZ', NOW(), NOW());
INSERT INTO `base_province` VALUES (33, '云南', '6', '530000', 'CN-53', 'CN-YN', NOW(), NOW());
INSERT INTO `base_province` VALUES (13, '重庆', '6', '500000', 'CN-50', 'CN-CQ', NOW(), NOW());
INSERT INTO `base_province` VALUES (34, '西藏', '6', '540000', 'CN-54', 'CN-X', NOW(), NOW());