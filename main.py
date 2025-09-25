import os
import shutil
import pandas as pd
from collections import defaultdict


def load_mapping(excel_path: str) -> dict:
    """读取 Excel，返回 {姓名: 住院流水号} 映射"""
    df = pd.read_excel(excel_path, dtype=str)
    df = df.fillna("")
    return dict(zip(df["姓名"], df["住院流水号"]))


def index_files(src_dir: str, extensions=None) -> list:
    """递归索引所有目标后缀文件"""
    if extensions is None:
        extensions = [".xml"]
    result = []
    for root, _, files in os.walk(src_dir):
        for f in files:
            if any(f.lower().endswith(ext) for ext in extensions):
                result.append(os.path.join(root, f))
    return result


import re

def parse_filename(filename: str) -> tuple:
    """
    解析文件名，提取文档分类 (SD-xx) 和姓名
    示例: EMR-SD-04-西药处方-李凤存-T01-001.xml
    返回: (SD-xx, 姓名)
    """
    parts = filename.split("-")

    category = None
    name = None

    try:
        sd_index = parts.index("SD")
        category = f"{parts[sd_index]}-{parts[sd_index + 1]}"  # SD-04
        # 姓名 = SD 后面 +2 的字段（跳过文档类型）
        if len(parts) > sd_index + 3:
            name = parts[sd_index + 3].strip()
    except Exception as e:
        print(f"⚠️ 解析文件名失败: {filename}, 错误: {e}")

    return category, name


def copy_all_files(dst_dir: str, mapping: dict, files: list):
    """复制全量文件，返回 patient_files 结构"""
    full_dir = os.path.join(dst_dir, "1.全量")
    os.makedirs(full_dir, exist_ok=True)

    patient_files = defaultdict(lambda: defaultdict(list))

    for src in files:
        fname = os.path.basename(src)
        category, name = parse_filename(fname)
        if not name or name not in mapping:
            print(f"⚠️ 未找到姓名映射: {name}, 文件跳过 {fname}")
            continue

        patient_id = mapping[name]
        new_dir = os.path.join(full_dir, f"{patient_id}-{name}", category)
        os.makedirs(new_dir, exist_ok=True)
        shutil.copy(src, os.path.join(new_dir, fname))

        patient_files[patient_id][category].append(src)

    return patient_files


def copy_limited_files(dst_dir: str, patient_files: dict, files: list):
    """复制部分文件（每人每类最多10个）"""
    part_dir = os.path.join(dst_dir, "2.部分")
    os.makedirs(part_dir, exist_ok=True)

    counter = defaultdict(lambda: defaultdict(int))

    for src in files:
        fname = os.path.basename(src)
        category, name = parse_filename(fname)
        if not name:
            continue

        for patient_id, cats in patient_files.items():
            if src in cats.get(category, []):
                if counter[patient_id][category] < 10:
                    new_dir = os.path.join(part_dir, f"{patient_id}-{name}", category)
                    os.makedirs(new_dir, exist_ok=True)
                    shutil.copy(src, os.path.join(new_dir, fname))
                    counter[patient_id][category] += 1
                break


def make_validation_set(dst_dir: str):
    """根据部分目录生成校验目录（按 SD-xx 分类归类），并打印统计"""
    part_dir = os.path.join(dst_dir, "2.部分")
    validation_dir = os.path.join(dst_dir, "3.校验")
    os.makedirs(validation_dir, exist_ok=True)

    stats = defaultdict(int)

    for patient in os.listdir(part_dir):
        patient_path = os.path.join(part_dir, patient)
        if not os.path.isdir(patient_path):
            continue
        for category in os.listdir(patient_path):
            category_path = os.path.join(patient_path, category)
            if not os.path.isdir(category_path):
                continue

            dst_path = os.path.join(validation_dir, category)
            os.makedirs(dst_path, exist_ok=True)

            for f in os.listdir(category_path):
                src_file = os.path.join(category_path, f)
                shutil.copy(src_file, os.path.join(dst_path, f))
                stats[category] += 1

    print("\n📊 校验集统计：")
    for category, count in sorted(stats.items()):
        print(f"  {category}: {count} 个")


def main():
    excel_path = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/第4轮/患者列表24-10.xlsx"
    src_dir = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/第4轮/文档下载"
    dst_dir = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/第4轮/文档整理"

    print("📌 开始读取 Excel 映射...")
    mapping = load_mapping(excel_path)

    print("📌 建立文件索引（递归多层目录，只处理 .xml 文件）...")
    file_index = index_files(src_dir, extensions=[".xml"])

    print(f"📌 共索引到 {len(file_index)} 个 .xml 文件")

    print("📌 复制全量文件...")
    patient_files = copy_all_files(dst_dir, mapping, file_index)

    print("📌 复制部分文件（每人每类最多10个）...")
    copy_limited_files(dst_dir, patient_files, file_index)

    print("📌 整理校验文件（按 SD-xx 分类归类）...")
    make_validation_set(dst_dir)

    print("✅ 文件整理完成！")


if __name__ == "__main__":
    main()