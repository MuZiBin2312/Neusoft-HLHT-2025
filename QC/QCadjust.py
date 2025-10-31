import re
import os

input_path = '/Users/lijiahe/Documents/Neusoft/proj/0929-专科专病质控/筛选新增.sql'
output_path = '/Users/lijiahe/Documents/Neusoft/proj/0929-专科专病质控/环节质控insert_modified.sql'

# === 尝试多种编码读取文件 ===
def read_file_try_encodings(path, encodings=['utf-8', 'gbk', 'latin1']):
    for enc in encodings:
        try:
            with open(path, 'r', encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    raise Exception("无法用常见编码读取文件，请检查文件编码类型。")

sql_text = read_file_try_encodings(input_path)

# === 匹配 insert 语句（忽略大小写 + 支持多行）===
pattern = re.compile(
    r"(insert\s+into\s+mqcmonitor\.qcm_score_item\s*\(.*?\)\s*values\s*\((.*?)\)\s*;)",
    re.IGNORECASE | re.DOTALL
)

matches = pattern.findall(sql_text)
modified_count = 0
modified_sql = sql_text
example_sql = None

for full_stmt, values_str in matches:
    # 拆分字段值（考虑逗号分隔，避免字符串内逗号干扰）
    parts = []
    buf = ""
    in_quote = False
    i = 0
    while i < len(values_str):
        ch = values_str[i]
        if ch == "'" and (i == 0 or values_str[i-1] != "\\"):
            in_quote = not in_quote
        if ch == "," and not in_quote:
            parts.append(buf.strip())
            buf = ""
        else:
            buf += ch
        i += 1
    parts.append(buf.strip())

    if len(parts) >= 10:
        parts[0] = "seq_qcm_maintaince_log.NEXTVAL"  # ID
        parts[2] = "568"                              # SUBITEM_ID
        parts[9] = "178"                              # STANDARD_ID

        new_values_str = ", ".join(parts)
        new_stmt = full_stmt.replace(values_str, new_values_str)

        modified_sql = modified_sql.replace(full_stmt, new_stmt)
        modified_count += 1
        if example_sql is None:
            example_sql = new_stmt

# === 写入输出文件 ===
os.makedirs(os.path.dirname(output_path), exist_ok=True)
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(modified_sql)

print(f"✅ 替换完成，结果已保存到：{output_path}")
print(f"🔢 共修改 {modified_count} 条 INSERT 语句。")
if modified_count > 0:
    print("\n🧩 示例（已修改）:")
    print(example_sql)
else:
    print("⚠️ 未匹配到任何 INSERT 语句，请检查文件是否包含目标表名。")