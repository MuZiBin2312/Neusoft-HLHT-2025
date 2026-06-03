import pandas as pd
import os


def generate_area_sql(input_excel_path, output_dir):
    """
    input_excel_path: Excel 文件的完整路径
    output_dir: SQL 脚本生成的文件夹路径
    """
    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_sql_path = os.path.join(output_dir, 'batch_insert_dcp_county.sql')

    try:
        # 1. 读取 Excel 文件
        # 如果您的 Excel 有多个 Sheet，可以指定 sheet_name
        df = pd.read_excel(input_excel_path)

        # 2. 定义层级映射关系
        level_map = {
            '国家级': '0',
            '省级': '1',
            '地市级': '2',
            '区县级': '3',
            '乡级': '4',
            '乡镇级': '4'
        }

        sql_lines = []

        # 3. 逐行处理数据
        for index, row in df.iterrows():
            # 提取字段并确保是字符串，去掉可能存在的 .0
            code = str(row['地区编码']).split('.')[0].strip()
            name = str(row['地区名称']).strip()
            level_name = str(row['地区级别']).strip()
            parent_code = str(row['上级地区编码']).split('.')[0].strip()

            # 映射 SORT_ID
            sort_id = level_map.get(level_name, '9')

            # 处理 MARK (上级编码)
            # 如果是全国(000000000)或省级，MARK 处理逻辑如下
            if level_name == '国家级' or parent_code in ['nan', '', '0']:
                mark_value = "NULL"
            else:
                mark_value = f"'{parent_code}'"

            # 4. 拼接 SQL 语句
            # 表名: com_dictionary, 类型: DCP_COUNTY.1, 编码: 9位保留
            sql = (
                f"INSERT INTO com_dictionary "
                f"(TYPE, CODE, NAME, SORT_ID, MARK, VALID_STATE, OPER_DATE, PARENT_CODE, CURRENT_CODE) "
                f"VALUES ('DCP_COUNTY.1', '{code}', '{name}', '{sort_id}', "
                f"{mark_value}, '1', SYSDATE, 'ROOT', 'ROOT');"
            )
            sql_lines.append(sql)

        # 5. 写入到 SQL 文件中
        with open(output_sql_path, 'w', encoding='utf-8') as f:
            # 写入一些基础设置（可选）
            f.write("-- 批量导入地区字典数据\n")
            f.write("SET DEFINE OFF;\n\n")

            f.write('\n'.join(sql_lines))

            f.write('\n\nCOMMIT;\n')

        print(f"成功！已处理 {len(sql_lines)} 条数据。")
        print(f"SQL 脚本已生成至: {output_sql_path}")

    except Exception as e:
        print(f"执行出错: {e}")


# --- 直接执行 ---
input_path = "/Users/lijiahe/Documents/Neusoft/proj/0400-死亡证/地区信息列表全国0402.xlsx"
output_path = "/Users/lijiahe/Documents/Neusoft/proj/0400-死亡证"

generate_area_sql(input_path, output_path)