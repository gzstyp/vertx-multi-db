/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.3.66_3309
 Source Server Type    : MySQL
 Source Server Version : 80023
 Source Host           : 192.168.3.66:3309
 Source Schema         : vertx

 Target Server Type    : MySQL
 Target Server Version : 80023
 File Encoding         : 65001

 Date: 04/06/2021 18:16:33
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for monitor_value
-- ----------------------------
DROP TABLE IF EXISTS `monitor_value`;
CREATE TABLE `monitor_value`  (
  `kid` bigint(0) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `device_flag` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '设备标识(MN设备唯一编码)',
  `volume` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '噪音音量值',
  `create_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建时间',
  PRIMARY KEY (`kid`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '监测点数据' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of monitor_value
-- ----------------------------
INSERT INTO `monitor_value` VALUES (1, '330901', '52.4', '2021-06-04 18:15:10');
INSERT INTO `monitor_value` VALUES (2, '330902', '65.12', '2021-06-04 18:15:25');
INSERT INTO `monitor_value` VALUES (3, '330903', '85.81', '2021-06-04 18:16:07');
INSERT INTO `monitor_value` VALUES (4, '330904', '50.20', '2021-06-04 18:16:21');
INSERT INTO `monitor_value` VALUES (5, '330905', '60.80', '2021-06-04 18:16:38');

SET FOREIGN_KEY_CHECKS = 1;
