import pandas as pd

# 科室对照表
dept_map = {
    "妇科": 1461,
    "产科": 1597,
    "消化科": [1042, 1047],
    "呼吸科": 1023,
    "血液科": 1610,
    "肾脏风湿科": 1052,
    "普外科": [1107, 1103],
    "神经内科": [1533, 1083],
    "眼科": 1305,
    "肾内科": 1052,
    "心内科": [1033, 1472, 1512],
    "胸外科": 1232,
    "泌尿外科": 1122,
    "甲乳外科": 1272,
    "肛肠科": [1192, 1204, 1202, 1203, 1182, 1205, 1209, 1213, 1206, 1207, 1208],
    "肿瘤科": [1507, 1492, 1502, 1212, 1509],
    "肿瘤内科": [1507, 1492, 1502, 1212, 1509],
    "感染免疫科": 1452
}

# Excel 文件路径
excel_path = "/Users/lijiahe/Documents/Neusoft/proj/0929-专科专病质控/专科专病质控规则 - 副本(1).xlsx"

# 读取 Excel
df = pd.read_excel(excel_path)

# 过滤条件：只取质控类别=专科 且 是否自动质控=否
df_filtered = df[(df["质控类别"] == "专科") & (df["是否自动质控"] == "否")]

sqls = []
sort_id = 1
matched_rules = []
unmatched_rules = []

for _, row in df_filtered.iterrows():
    rule = str(row["质控规则"]).strip()
    record_item = str(row["归属"]).strip()

    # 提取括号里的科室
    if "（" in rule and "）" in rule:
        dept = rule.split("（")[1].split("）")[0]
    else:
        unmatched_rules.append(rule)
        continue

    if dept not in dept_map:
        unmatched_rules.append(rule)
        continue

    dept_ids = dept_map[dept]
    if not isinstance(dept_ids, list):
        dept_ids = [dept_ids]

    dept_str = ",".join(str(d) for d in dept_ids)

    sql_item = f"""-- {rule}
INSERT INTO QCM_SCORE_ITEM
(ID, NAME, SUBITEM_ID, IS_VALID, DEDUCT_TYPE, SCORE, VETO_LEVEL, SORT_ID, ISAUTO, STANDARD_ID, RULE_APPLY_ID, CLASSIFY_ID, MAX_SCORE, IS_CALL, APPLICABLE_DEPT_CODE, APPLICABLE_DIAGNOSE, WEIGHT_SCORE, IS_AUTOREMIND, APPLICABLE_RECORDITEM) 
VALUES
(seq_qcm_score_item.NEXTVAL, '{rule}', 499, 1, 'O', 1, '', {sort_id}, 0, 182, NULL, 202, 0, 0, '{dept_str}', 'All', 0, NULL, '{record_item}');

INSERT INTO qcm_maintaince_log
(ID, oper_table, oper_field, oper_rowid, oper_type, old_value, new_value, oper_code, oper_date, oper_ip) 
VALUES
(seq_qcm_maintaince_log.NEXTVAL, 'QCM_SCORE_ITEM', 'ID', NULL, 'Add', '', TO_CHAR(seq_qcm_score_item.CURRVAL), '100464', SYSDATE, '200.200.199.101');
"""
    sqls.append(sql_item)
    matched_rules.append(rule)
    sort_id += 1

# 保存 SQL
with open("../insert_rules.sql", "w", encoding="utf-8") as f:
    f.write("\n".join(sqls))

# 输出统计信息
print("✅ SQL 已生成到 insert_rules.sql")
print(f"📊 总规则数（符合条件）: {len(df_filtered)}")
print(f"✅ 成功匹配规则数: {len(matched_rules)}")
print(f"❌ 未匹配规则数: {len(unmatched_rules)}")
if unmatched_rules:
    print("未匹配的规则如下：")
    for rule in unmatched_rules:
        print(f"- {rule}")