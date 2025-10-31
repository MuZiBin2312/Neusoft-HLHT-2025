import re
import pandas as pd

# === 配置区 ===
sql_path = '/Users/lijiahe/Documents/Neusoft/proj/0929-专科专病质控/筛选新增.sql'
excel_path = '/Users/lijiahe/Documents/Neusoft/proj/0929-专科专病质控/副本专科专病质控规则 - 副本(1).xlsx'
sheet_name = 0  # 如果 Excel 有多个 sheet，可改成名字或索引
output_path = '/Users/lijiahe/Documents/Neusoft/proj/0929-专科专病质控/sql_vs_excel_diff.xlsx'

# === 1. 读取 Excel 中的质控规则列 ===
df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
excel_rules = df['质控规则'].dropna().str.strip().tolist()
excel_set = set(excel_rules)

# === 2. 从 SQL 文件中提取所有 INSERT 的 NAME 字段内容 ===
# 自动尝试多种编码读取
def read_sql_file(path):
    for enc in ['utf-8', 'gbk', 'ansi', 'latin1']:
        try:
            with open(path, 'r', encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("无法正确解码 SQL 文件，请确认文件编码。")

sql_text = read_sql_file(sql_path)

# 匹配类似: VALUES(..., '死亡时间不具体或病历中死亡时间不一致', ...
pattern = re.compile(r"VALUES\s*\([^']*'([^']+)'\s*,", re.IGNORECASE)
sql_names = pattern.findall(sql_text)
sql_set = set(name.strip() for name in sql_names)

# === 3. 比对差异 ===
missing_in_excel = sorted(list(sql_set - excel_set))  # SQL 里有但 Excel 没有的
missing_in_sql = sorted(list(excel_set - sql_set))    # Excel 里有但 SQL 没有的

# === 4. 输出结果 ===
result_df = pd.DataFrame({
    'SQL中存在但Excel缺失': pd.Series(missing_in_excel),
    'Excel中存在但SQL缺失': pd.Series(missing_in_sql)
})

result_df.to_excel(output_path, index=False)
print(f"✅ 比对完成，结果已保存到：{output_path}")
print(f"SQL条数: {len(sql_set)}, Excel条数: {len(excel_set)}")
print(f"SQL多出: {len(missing_in_excel)} 条, Excel多出: {len(missing_in_sql)} 条")