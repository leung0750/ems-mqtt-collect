/*
 Navicat Premium Dump SQL

 Source Server         : gdnx
 Source Server Type    : MySQL
 Source Server Version : 80042 (8.0.42)
 Source Host           : frp.gd-nx.com:11010
 Source Schema         : energy_gdjl

 Target Server Type    : MySQL
 Target Server Version : 80042 (8.0.42)
 File Encoding         : 65001

 Date: 08/01/2026 13:49:10
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for collect_energy
-- ----------------------------
DROP TABLE IF EXISTS `collect_energy`;
CREATE TABLE `collect_energy`  (
  `collect_device_name` varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `collect_device_id` int NOT NULL,
  `gateway_no` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `collect_time` datetime NOT NULL,
  `sys_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `f` decimal(20, 4) NULL DEFAULT NULL,
  `fa` decimal(20, 4) NULL DEFAULT NULL,
  `fb` decimal(20, 4) NULL DEFAULT NULL,
  `fc` decimal(20, 4) NULL DEFAULT NULL,
  `ia` decimal(20, 4) NULL DEFAULT NULL,
  `ib` decimal(20, 4) NULL DEFAULT NULL,
  `ic` decimal(20, 4) NULL DEFAULT NULL,
  `p` decimal(20, 4) NULL DEFAULT NULL,
  `pa` decimal(20, 4) NULL DEFAULT NULL,
  `pb` decimal(20, 4) NULL DEFAULT NULL,
  `pc` decimal(20, 4) NULL DEFAULT NULL,
  `pw` decimal(20, 4) NULL DEFAULT NULL,
  `px` decimal(20, 4) NULL DEFAULT NULL,
  `q` decimal(20, 4) NULL DEFAULT NULL,
  `qw` decimal(20, 4) NULL DEFAULT NULL,
  `qx` decimal(20, 4) NULL DEFAULT NULL,
  `s` decimal(20, 4) NULL DEFAULT NULL,
  `sw` decimal(20, 4) NULL DEFAULT NULL,
  `sx` decimal(20, 4) NULL DEFAULT NULL,
  `ua` decimal(20, 4) NULL DEFAULT NULL,
  `uab` decimal(20, 4) NULL DEFAULT NULL,
  `uac` decimal(20, 4) NULL DEFAULT NULL,
  `ub` decimal(20, 4) NULL DEFAULT NULL,
  `ubc` decimal(20, 4) NULL DEFAULT NULL,
  `uc` decimal(20, 4) NULL DEFAULT NULL,
  `hz` decimal(20, 4) NULL DEFAULT NULL,
  `pr` decimal(20, 4) NULL DEFAULT NULL,
  `sf` decimal(20, 4) NULL DEFAULT NULL,
  `st` decimal(20, 4) NULL DEFAULT NULL,
  `tp` decimal(20, 4) NULL DEFAULT NULL,
  `wf` decimal(20, 4) NULL DEFAULT NULL,
  `tp_a` decimal(20, 4) NULL DEFAULT NULL,
  `tp_b` decimal(20, 4) NULL DEFAULT NULL,
  `tp_c` decimal(20, 4) NULL DEFAULT NULL,
  `a` decimal(20, 4) NULL DEFAULT NULL,
  `u` decimal(20, 4) NULL DEFAULT NULL,
  `avg_u` decimal(20, 4) NULL DEFAULT NULL,
  `avg_uu` decimal(20, 4) NULL DEFAULT NULL,
  `avg_i` decimal(20, 4) NULL DEFAULT NULL,
  `z_i` decimal(20, 4) NULL DEFAULT NULL,
  `pw_fan` decimal(20, 4) NULL DEFAULT NULL,
  `qw_fan` decimal(20, 4) NULL DEFAULT NULL,
  `fr` decimal(20, 4) NULL DEFAULT NULL,
  `dx` decimal(20, 4) NULL DEFAULT NULL,
  `dy` decimal(20, 4) NULL DEFAULT NULL,
  `dz` decimal(20, 4) NULL DEFAULT NULL,
  `create_time` datetime NOT NULL,
  `energy_type` smallint NOT NULL,
  `vx` decimal(20, 4) NULL DEFAULT NULL,
  `vy` decimal(20, 4) NULL DEFAULT NULL,
  `vz` decimal(20, 4) NULL DEFAULT NULL,
  PRIMARY KEY (`collect_device_id`, `collect_time`) USING BTREE,
  INDEX `collectenergy_collect_time`(`collect_time` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
