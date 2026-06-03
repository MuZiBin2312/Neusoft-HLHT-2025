import pandas as pd
import datetime


def generate_sql_from_excel(excel_path, output_sql_path):
    # 1. 读取 Excel 文件中的第一个工作表
    df = pd.read_excel(excel_path, sheet_name='SQL Results')

    # 2. 定义基础固定参数
    parent_id = 1259566784857972736
    base_id = 1259566905385492481  # 从你给的下一位 ID 开始递增
    dict_type_code = 'NHSA_HI_ORG_CODE'
    create_user = 'test001'
    modify_user = 'test001'

    # 严格匹配你要求的 2026 年 6 月 3 日下午的格式
    # 格式示例：03-6月 -26 03.10.17.496000 下午 -> 这里采用通用兼容的 TO_TIMESTAMP
    current_time_str = "2026-06-03 04:13:00 下午"

    sql_lines = []

    # 3. 循环遍历 Excel 中的每一行数据
    for index, row in df.iterrows():
        # 安全获取 Excel 中的编码（DICT_DETAIL_CODE）和名称（DICT_DETAIL_NAME）
        detail_code = str(row['编码']).strip()
        detail_name = str(row['名称']).strip()
        remarks = str(row['代码']).strip()  # "国家医保局医疗机构代码"

        # 核心字段计算
        unique_id = str(base_id + index)
        sort_no = index + 1
        enable_flag = 0
        cacheable = 0

        # 4. 严格按照你给的字段顺序和空格生成 SQL
        sql = f"INSERT INTO nmrws.nmrws_system_dict_detail (ID, DICT_DETAIL_CODE, DICT_DETAIL_NAME, DICT_TYPE_CODE, PARENT_ID, REMARKS, CREATE_USER, CREATE_TIME, MODIFY_USER, MODIFY_TIME, ENABLE_FLAG, SORT_NO, CACHEABLE) \n" \
              f"VALUES ('{unique_id}', '{detail_code}', '{detail_name}', '{dict_type_code}', '{parent_id}', '{remarks}', '{create_user}', TO_TIMESTAMP('{current_time_str}', 'YYYY-MM-DD HH12:MI:SS AM'), '{modify_user}', TO_TIMESTAMP('{current_time_str}', 'YYYY-MM-DD HH12:MI:SS AM'), {enable_flag}, {sort_no}, {cacheable});"


        sql_lines.append(sql)

    # 5. 将生成的 SQL 语句写入文件
    with open(output_sql_path, 'w', encoding='utf-8') as f:
        f.write("\n\n".join(sql_lines))

    print(f"成功！已从 {excel_path} 读取了 {len(df)} 条医疗机构数据，并生成了脚本：{output_sql_path}")


# --- 运行转换 ---
# --- 运行转换 ---
if __name__ == "__main__":
    # 1. 设置输入的 Excel 文件路径（可以是绝对路径，注意斜杠）
    # 比如：'D:/Neusoft-HLHT-2025/GenInsert/医疗机构(1).xlsx'
    input_excel = '/Users/lijiahe/Documents/deskWorkspace/医疗机构(1).xlsx'

    # 2. 设置输出的 SQL 文件路径
    # 比如：'D:/Neusoft-HLHT-2025/GenInsert/insert_medical_orgs.sql'
    output_sql = '/Users/lijiahe/Documents/deskWorkspace/insert_medical_orgs.sql'

    # 运行函数
    generate_sql_from_excel(input_excel, output_sql)