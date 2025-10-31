-- （产科） 正常分娩病人无早期产检情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科） 正常分娩病人无早期产检情况描述', 499, 1, 'O', 1, '', 1, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科） 正常分娩病人无早孕反应描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科） 正常分娩病人无早孕反应描述', 499, 1, 'O', 1, '', 2, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）产科检查中无腹围
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）产科检查中无腹围', 499, 1, 'O', 1, '', 3, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）产科检查中无宫高
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）产科检查中无宫高', 499, 1, 'O', 1, '', 4, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）产科检查中无入盆情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）产科检查中无入盆情况描述', 499, 1, 'O', 1, '', 5, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）产科检查中无胎方位
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）产科检查中无胎方位', 499, 1, 'O', 1, '', 6, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）婚育月经史中有无记录近亲结婚，足月、早产、流产、异常孕产情况等描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）婚育月经史中有无记录近亲结婚，足月、早产、流产、异常孕产情况等描述', 499, 1, 'O', 1, '', 7, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）检查胎心次数有无描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）检查胎心次数有无描述', 499, 1, 'O', 1, '', 8, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）入院记录病历专科情况需描述胎动
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）入院记录病历专科情况需描述胎动', 499, 1, 'O', 1, '', 9, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）入院记录病历专科情况需描述孕龄
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）入院记录病历专科情况需描述孕龄', 499, 1, 'O', 1, '', 10, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）入院记录产科：妊娠分娩史内容不能为空
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）入院记录产科：妊娠分娩史内容不能为空', 499, 1, 'O', 1, '', 11, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）入院记录辅助检查中没有OGTT检查结果
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）入院记录辅助检查中没有OGTT检查结果', 499, 1, 'O', 1, '', 12, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）入院记录体格检查高危评定产科高危情况空项
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）入院记录体格检查高危评定产科高危情况空项', 499, 1, 'O', 1, '', 13, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）入院记录现病史末次月经与预产期不符合
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）入院记录现病史末次月经与预产期不符合', 499, 1, 'O', 1, '', 14, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）是否记录建围产保健卡情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）是否记录建围产保健卡情况', 499, 1, 'O', 1, '', 15, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）现病史中没记录产妇产前并发症
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）现病史中没记录产妇产前并发症', 499, 1, 'O', 1, '', 16, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）预产期未记录
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）预产期未记录', 499, 1, 'O', 1, '', 17, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）月经史中未记录月经量以及形状情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）月经史中未记录月经量以及形状情况', 499, 1, 'O', 1, '', 18, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）专科情况宫颈大小不可为空
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）专科情况宫颈大小不可为空', 499, 1, 'O', 1, '', 19, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （产科）专科情况先露部位不可为空
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（产科）专科情况先露部位不可为空', 499, 1, 'O', 1, '', 20, 0, 182, NULL, 202, 0, 0, '1597', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （妇科）病历要书写阴道子宫检查
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（妇科）病历要书写阴道子宫检查', 499, 1, 'O', 1, '', 21, 0, 182, NULL, 202, 0, 0, '1461', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （妇科）已婚患者专科检查中缺少阴道分泌物情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（妇科）已婚患者专科检查中缺少阴道分泌物情况描述', 499, 1, 'O', 1, '', 22, 0, 182, NULL, 202, 0, 0, '1461', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （妇科）已婚患者专科检查中缺少阴道顺畅情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（妇科）已婚患者专科检查中缺少阴道顺畅情况描述', 499, 1, 'O', 1, '', 23, 0, 182, NULL, 202, 0, 0, '1461', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （感染免疫科）既往史诊断疾病后疫苗接种情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（感染免疫科）既往史诊断疾病后疫苗接种情况', 499, 1, 'O', 1, '', 24, 0, 182, NULL, 202, 0, 0, '1452', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （感染免疫科）现病史缺少关节肿瘤、腹痛、有无便血、尿色尿量变化、感染等伴随症状描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（感染免疫科）现病史缺少关节肿瘤、腹痛、有无便血、尿色尿量变化、感染等伴随症状描述', 499, 1, 'O', 1, '', 25, 0, 182, NULL, 202, 0, 0, '1452', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （感染免疫科）现病史缺少具体诊疗过程：化验查验、治疗过程（具体用药时间、剂量、时间）
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（感染免疫科）现病史缺少具体诊疗过程：化验查验、治疗过程（具体用药时间、剂量、时间）', 499, 1, 'O', 1, '', 26, 0, 182, NULL, 202, 0, 0, '1452', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （感染免疫科）现病史缺少皮疹部位及具体变化情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（感染免疫科）现病史缺少皮疹部位及具体变化情况', 499, 1, 'O', 1, '', 27, 0, 182, NULL, 202, 0, 0, '1452', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （感染免疫科）现病史缺少有无剧烈活动、外伤、感染、特殊用药情况及有无腹痛、腹背部痛、皮疹、头痛头晕、视物不清等情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（感染免疫科）现病史缺少有无剧烈活动、外伤、感染、特殊用药情况及有无腹痛、腹背部痛、皮疹、头痛头晕、视物不清等情况描述', 499, 1, 'O', 1, '', 28, 0, 182, NULL, 202, 0, 0, '1452', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肛肠科）专科检查中肛门检查情况有无描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肛肠科）专科检查中肛门检查情况有无描述', 499, 1, 'O', 1, '', 29, 0, 182, NULL, 202, 0, 0, '1192,1204,1202,1203,1182,1205,1209,1213,1206,1207,1208', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （呼吸科）缺肺部叩诊查体
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（呼吸科）缺肺部叩诊查体', 499, 1, 'O', 1, '', 30, 0, 182, NULL, 202, 0, 0, '1023', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （呼吸科）缺肺部听诊查体
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（呼吸科）缺肺部听诊查体', 499, 1, 'O', 1, '', 31, 0, 182, NULL, 202, 0, 0, '1023', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （呼吸科）缺呼吸频次
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（呼吸科）缺呼吸频次', 499, 1, 'O', 1, '', 32, 0, 182, NULL, 202, 0, 0, '1023', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （呼吸科）缺吸烟史
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（呼吸科）缺吸烟史', 499, 1, 'O', 1, '', 33, 0, 182, NULL, 202, 0, 0, '1023', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （甲乳外科）入院记录专科检查中有无描述甲状腺查体情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（甲乳外科）入院记录专科检查中有无描述甲状腺查体情况', 499, 1, 'O', 1, '', 34, 0, 182, NULL, 202, 0, 0, '1272', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （甲乳外科）入院记录专科检查中有无乳腺检查结果描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（甲乳外科）入院记录专科检查中有无乳腺检查结果描述', 499, 1, 'O', 1, '', 35, 0, 182, NULL, 202, 0, 0, '1272', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （泌尿外科）专科检查中有无肾区、输尿管走形区及外因相关检查描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（泌尿外科）专科检查中有无肾区、输尿管走形区及外因相关检查描述', 499, 1, 'O', 1, '', 36, 0, 182, NULL, 202, 0, 0, '1122', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （泌尿外科）专科检查中有无直肠指诊及前列腺相关检查描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（泌尿外科）专科检查中有无直肠指诊及前列腺相关检查描述', 499, 1, 'O', 1, '', 37, 0, 182, NULL, 202, 0, 0, '1122', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （普外科）腹痛部位、性质的描述（如钝痛、锐痛等），伴随症状：呕吐、呕吐物形状、量、次数；发热频次、类型
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（普外科）腹痛部位、性质的描述（如钝痛、锐痛等），伴随症状：呕吐、呕吐物形状、量、次数；发热频次、类型', 499, 1, 'O', 1, '', 38, 0, 182, NULL, 202, 0, 0, '1107,1103', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （普外科）首次病程与主治医师查房内容应有所区别，不能完全一样，应体现三级医师查房制度
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（普外科）首次病程与主治医师查房内容应有所区别，不能完全一样，应体现三级医师查房制度', 499, 1, 'O', 1, '', 39, 0, 182, NULL, 202, 0, 0, '1107,1103', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （普外科）所有化验检查病程中应记录并详细分析记录化验检查结果
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（普外科）所有化验检查病程中应记录并详细分析记录化验检查结果', 499, 1, 'O', 1, '', 40, 0, 182, NULL, 202, 0, 0, '1107,1103', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （普外科）专科检查中腹部视触叩听诊有无描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（普外科）专科检查中腹部视触叩听诊有无描述', 499, 1, 'O', 1, '', 41, 0, 182, NULL, 202, 0, 0, '1107,1103', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）（颅脑外伤）现病史无受伤经过和伤后处理情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）（颅脑外伤）现病史无受伤经过和伤后处理情况', 499, 1, 'O', 1, '', 42, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）个人史中是否记录出生史、喂养史、生长发育史、预防接种史情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）个人史中是否记录出生史、喂养史、生长发育史、预防接种史情况', 499, 1, 'O', 1, '', 43, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）患者首次病程记录缺失定位诊断
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）患者首次病程记录缺失定位诊断', 499, 1, 'O', 1, '', 44, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）患者首次病程记录缺失定性诊断
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）患者首次病程记录缺失定性诊断', 499, 1, 'O', 1, '', 45, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）入院记录体格检查神经系统专科查体空项
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）入院记录体格检查神经系统专科查体空项', 499, 1, 'O', 1, '', 46, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）入院记录现病史 神经系统查体相关症状持续时间未写
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）入院记录现病史 神经系统查体相关症状持续时间未写', 499, 1, 'O', 1, '', 47, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）入院记录专科检查中有误神志描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）入院记录专科检查中有误神志描述', 499, 1, 'O', 1, '', 48, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）专科情况无反射活动
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）专科情况无反射活动', 499, 1, 'O', 1, '', 49, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）专科情况无共济运动
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）专科情况无共济运动', 499, 1, 'O', 1, '', 50, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）专科情况无肌力检查
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）专科情况无肌力检查', 499, 1, 'O', 1, '', 51, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）专科情况无颅神经检查
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）专科情况无颅神经检查', 499, 1, 'O', 1, '', 52, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （神经内科）专科情况无神经系统检查
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（神经内科）专科情况无神经系统检查', 499, 1, 'O', 1, '', 53, 0, 182, NULL, 202, 0, 0, '1533,1083', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肾内科）既往史：肾病疾病相关既往史记录不全
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肾内科）既往史：肾病疾病相关既往史记录不全', 499, 1, 'O', 1, '', 54, 0, 182, NULL, 202, 0, 0, '1052', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肾内科）体格检查肾内专科查体记录不规范或漏项
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肾内科）体格检查肾内专科查体记录不规范或漏项', 499, 1, 'O', 1, '', 55, 0, 182, NULL, 202, 0, 0, '1052', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肾内科）现病史“水肿、小便异常、高血压”等肾内专科重点情况记录不全
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肾内科）现病史“水肿、小便异常、高血压”等肾内专科重点情况记录不全', 499, 1, 'O', 1, '', 56, 0, 182, NULL, 202, 0, 0, '1052', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肾内科）现病史缺少具体尿色及变化情况、是否全程血尿、尿中有无血丝学块沉渣、尿路刺激症状、水肿少尿等情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肾内科）现病史缺少具体尿色及变化情况、是否全程血尿、尿中有无血丝学块沉渣、尿路刺激症状、水肿少尿等情况描述', 499, 1, 'O', 1, '', 57, 0, 182, NULL, 202, 0, 0, '1052', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （消化科）(呕血)患者无腹部叩诊情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（消化科）(呕血)患者无腹部叩诊情况', 499, 1, 'O', 1, '', 58, 0, 182, NULL, 202, 0, 0, '1042,1047', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （消化科）(呕血)无腹部触诊情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（消化科）(呕血)无腹部触诊情况', 499, 1, 'O', 1, '', 59, 0, 182, NULL, 202, 0, 0, '1042,1047', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （消化科）(呕血)无饮酒史
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（消化科）(呕血)无饮酒史', 499, 1, 'O', 1, '', 60, 0, 182, NULL, 202, 0, 0, '1042,1047', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （消化科）便血病人现病史缺少便与血的关系描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（消化科）便血病人现病史缺少便与血的关系描述', 499, 1, 'O', 1, '', 61, 0, 182, NULL, 202, 0, 0, '1042,1047', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （消化科）现病史缺少腹痛性质、大便形状及频次描述，脱水缺少口渴、尿少症状描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（消化科）现病史缺少腹痛性质、大便形状及频次描述，脱水缺少口渴、尿少症状描述', 499, 1, 'O', 1, '', 62, 0, 182, NULL, 202, 0, 0, '1042,1047', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （消化科）现病史缺少皮疹的变化、伴随症状如痒感描述；体格检查缺少淋巴结特点（大小、部位、质地、外观等）
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（消化科）现病史缺少皮疹的变化、伴随症状如痒感描述；体格检查缺少淋巴结特点（大小、部位、质地、外观等）', 499, 1, 'O', 1, '', 63, 0, 182, NULL, 202, 0, 0, '1042,1047', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （消化科）心衰者现病史缺少症状描述：尿少、汗多、活动量下降、哺乳间歇等描述；查体缺少肝脏大小、质地描述、末梢循环描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（消化科）心衰者现病史缺少症状描述：尿少、汗多、活动量下降、哺乳间歇等描述；查体缺少肝脏大小、质地描述、末梢循环描述', 499, 1, 'O', 1, '', 64, 0, 182, NULL, 202, 0, 0, '1042,1047', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （心内科）体格检查无心包摩擦感
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（心内科）体格检查无心包摩擦感', 499, 1, 'O', 1, '', 65, 0, 182, NULL, 202, 0, 0, '1033,1472,1512', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （心内科）体格检查无心尖搏动点
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（心内科）体格检查无心尖搏动点', 499, 1, 'O', 1, '', 66, 0, 182, NULL, 202, 0, 0, '1033,1472,1512', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （心内科）体格检查无心律
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（心内科）体格检查无心律', 499, 1, 'O', 1, '', 67, 0, 182, NULL, 202, 0, 0, '1033,1472,1512', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （心内科）体格检查无心率
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（心内科）体格检查无心率', 499, 1, 'O', 1, '', 68, 0, 182, NULL, 202, 0, 0, '1033,1472,1512', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （心内科）体格检查无心前区隆起
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（心内科）体格检查无心前区隆起', 499, 1, 'O', 1, '', 69, 0, 182, NULL, 202, 0, 0, '1033,1472,1512', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （胸外科）体格检查无呼吸音情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（胸外科）体格检查无呼吸音情况描述', 499, 1, 'O', 1, '', 70, 0, 182, NULL, 202, 0, 0, '1232', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （胸外科）体格检查无胸廓情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（胸外科）体格检查无胸廓情况描述', 499, 1, 'O', 1, '', 71, 0, 182, NULL, 202, 0, 0, '1232', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （血液科）既往史未描述直系亲属有无血液系统疾病
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（血液科）既往史未描述直系亲属有无血液系统疾病', 499, 1, 'O', 1, '', 72, 0, 182, NULL, 202, 0, 0, '1610', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （血液科）现病史缺少有无鼻出血、尿血、便血等描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（血液科）现病史缺少有无鼻出血、尿血、便血等描述', 499, 1, 'O', 1, '', 73, 0, 182, NULL, 202, 0, 0, '1610', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （血液科）现病史缺少有无骨痛、关节肿痛的描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（血液科）现病史缺少有无骨痛、关节肿痛的描述', 499, 1, 'O', 1, '', 74, 0, 182, NULL, 202, 0, 0, '1610', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （血液科）现病史缺少有无黄疸的描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（血液科）现病史缺少有无黄疸的描述', 499, 1, 'O', 1, '', 75, 0, 182, NULL, 202, 0, 0, '1610', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （血液科）现病史缺少有无头晕、头痛、皮疹的描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（血液科）现病史缺少有无头晕、头痛、皮疹的描述', 499, 1, 'O', 1, '', 76, 0, 182, NULL, 202, 0, 0, '1610', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （眼科）入院记录既往史 缺眼疾相关的劵商疾病史
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（眼科）入院记录既往史 缺眼疾相关的劵商疾病史', 499, 1, 'O', 1, '', 77, 0, 182, NULL, 202, 0, 0, '1305', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （眼科）眼科入院记录：眼科专科检查不能为空
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（眼科）眼科入院记录：眼科专科检查不能为空', 499, 1, 'O', 1, '', 78, 0, 182, NULL, 202, 0, 0, '1305', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （眼科）眼科入院记录：眼科专科检查不能为空
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（眼科）眼科入院记录：眼科专科检查不能为空', 499, 1, 'O', 1, '', 79, 0, 182, NULL, 202, 0, 0, '1305', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肿瘤科）病史中病理检查结果不可为空
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肿瘤科）病史中病理检查结果不可为空', 499, 1, 'O', 1, '', 80, 0, 182, NULL, 202, 0, 0, '1507,1492,1502,1212,1509', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肿瘤科）病史中分子病理结果内容不可为空
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肿瘤科）病史中分子病理结果内容不可为空', 499, 1, 'O', 1, '', 81, 0, 182, NULL, 202, 0, 0, '1507,1492,1502,1212,1509', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肿瘤科）无既往肿瘤相关诊断、诊断时间、分期、检查情况、治疗情况、ECOG体力情况评分情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肿瘤科）无既往肿瘤相关诊断、诊断时间、分期、检查情况、治疗情况、ECOG体力情况评分情况', 499, 1, 'O', 1, '', 82, 0, 182, NULL, 202, 0, 0, '1507,1492,1502,1212,1509', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肿瘤科）专科情况没有描述TNM分期情况
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肿瘤科）专科情况没有描述TNM分期情况', 499, 1, 'O', 1, '', 83, 0, 182, NULL, 202, 0, 0, '1507,1492,1502,1212,1509', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肿瘤内科）体格检查没有全身淋巴结情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肿瘤内科）体格检查没有全身淋巴结情况描述', 499, 1, 'O', 1, '', 84, 0, 182, NULL, 202, 0, 0, '1507,1492,1502,1212,1509', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肿瘤内科）现病史没有病理类型描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肿瘤内科）现病史没有病理类型描述', 499, 1, 'O', 1, '', 85, 0, 182, NULL, 202, 0, 0, '1507,1492,1502,1212,1509', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');

-- （肿瘤内科）现病史没有镇痛药物使用情况描述
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '（肿瘤内科）现病史没有镇痛药物使用情况描述', 499, 1, 'O', 1, '', 86, 0, 182, NULL, 202, 0, 0, '1507,1492,1502,1212,1509', 'All', 0, NULL, '入院记录');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');
