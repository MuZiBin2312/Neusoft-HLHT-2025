import os
import shutil
import re
import xml.etree.ElementTree as ET
import pandas as pd
from collections import defaultdict


def load_mapping(excel_path: str) -> dict:
    df = pd.read_excel(excel_path, dtype=str).fillna("")
    mapping = dict(zip(df["姓名"], df["住院流水号"]))
    print(f"📌 Excel 映射加载完成: {len(mapping)} 条")
    return mapping


def index_files(src_dir: str, extensions=None) -> list:
    if extensions is None:
        extensions = [".xml"]
    result = []
    for root, _, files in os.walk(src_dir):
        for f in files:
            if any(f.lower().endswith(ext) for ext in extensions):
                result.append(os.path.join(root, f))
    return result


def parse_category(filename: str) -> str:
    parts = filename.split("-")
    for i, p in enumerate(parts):
        if p.upper() == "SD" and i + 1 < len(parts) and parts[i + 1].isdigit():
            return f"SD-{parts[i + 1]}"
        elif re.match(r"SD\d+", p.upper()):
            return p.upper()
        elif re.match(r"SD-\d+", p.upper()):
            return p.upper()
    return "UNKNOWN"


def extract_patient_id_from_xml(filepath: str) -> str:
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        id_node = root.find(".//id[@root='2.16.156.10011.1.1']")
        if id_node is not None and "extension" in id_node.attrib:
            return id_node.attrib["extension"]
    except Exception as e:
        print(f"⚠️ XML解析失败: {filepath}, 错误: {e}")
    return "UNKNOWN"


def parse_patient_id_and_name(filepath: str, mapping: dict) -> tuple:
    fname = os.path.basename(filepath)
    category = parse_category(fname)
    parts = fname.split("-")

    # SD-04和SD-05 → 从文件名找姓名（跳过类型名）
    if category in ["SD-04", "SD-05"]:
        if len(parts) >= 5:
            name = parts[4]
        else:
            name = "未知姓名"
        patient_id = mapping.get(name, "UNKNOWN")
        if patient_id == "UNKNOWN":
            print(f"⚠️ {category} 找不到患者号: {name}")
        else:
            print(f"🛠 {category} 用Excel补全: {name} → {patient_id}")
        return patient_id, name

    # 其他类型 → 优先路径提取
    m = re.search(r"(ZY\d+)-([^/\\]+)", filepath)
    if m:
        return m.group(1), m.group(2)

    # 没有路径提取 → XML 提取
    if len(parts) >= 4:
        name = parts[3]
    else:
        name = "未知姓名"
    patient_id = extract_patient_id_from_xml(filepath)
    return patient_id, name


def copy_all_files(dst_dir: str, files: list, mapping: dict):
    full_dir = os.path.join(dst_dir, "1.全量")
    os.makedirs(full_dir, exist_ok=True)

    patient_files = defaultdict(lambda: defaultdict(list))

    for src in files:
        fname = os.path.basename(src)
        patient_id, name = parse_patient_id_and_name(src, mapping)
        category = parse_category(fname)

        if patient_id == "UNKNOWN":
            print(f"⚠️ 未找到患者号: {fname}")

        new_dir = os.path.join(full_dir, f"{patient_id}-{name}", category)
        os.makedirs(new_dir, exist_ok=True)
        shutil.copy(src, os.path.join(new_dir, fname))

        patient_files[patient_id][category].append(src)

    return patient_files


def copy_limited_files(dst_dir: str, patient_files: dict, mapping: dict):
    part_dir = os.path.join(dst_dir, "2.部分")
    os.makedirs(part_dir, exist_ok=True)

    counter = defaultdict(lambda: defaultdict(int))

    for patient_id, cats in patient_files.items():
        for category, files in cats.items():
            for src in files:
                if counter[patient_id][category] < 10:
                    fname = os.path.basename(src)
                    _, name = parse_patient_id_and_name(src, mapping)
                    new_dir = os.path.join(part_dir, f"{patient_id}-{name}", category)
                    os.makedirs(new_dir, exist_ok=True)
                    shutil.copy(src, os.path.join(new_dir, fname))
                    counter[patient_id][category] += 1


def make_validation_set(dst_dir: str):
    part_dir = os.path.join(dst_dir, "2.部分")
    validation_dir = os.path.join(dst_dir, "3.校验")
    os.makedirs(validation_dir, exist_ok=True)

    stats = defaultdict(int)
    category_files = defaultdict(list)

    # 收集所有文件按分类
    for patient in os.listdir(part_dir):
        patient_path = os.path.join(part_dir, patient)
        if not os.path.isdir(patient_path):
            continue
        for category in os.listdir(patient_path):
            category_path = os.path.join(patient_path, category)
            if not os.path.isdir(category_path):
                continue
            for f in os.listdir(category_path):
                if f.lower().endswith(".xml"):
                    category_files[category].append(os.path.join(category_path, f))

    # 按分类拆分
    for category, files in category_files.items():
        total_files = len(files)
        dst_category_path = os.path.join(validation_dir, category)
        os.makedirs(dst_category_path, exist_ok=True)

        if total_files > 100:
            # 平分逻辑
            num_folders = (total_files + 99) // 100  # 最少分多少组
            base_size = total_files // num_folders
            remainder = total_files % num_folders
            start = 0

            for i in range(num_folders):
                subfolder = os.path.join(dst_category_path, f"{i+1}")
                os.makedirs(subfolder, exist_ok=True)

                # 平分：前 remainder 组多一个文件
                size = base_size + (1 if i < remainder else 0)
                batch_files = files[start:start + size]
                start += size

                for fpath in batch_files:
                    shutil.copy(fpath, os.path.join(subfolder, os.path.basename(fpath)))
                stats[f"{category}/{i+1}"] = len(batch_files)
        else:
            for fpath in files:
                shutil.copy(fpath, os.path.join(dst_category_path, os.path.basename(fpath)))
            stats[category] = total_files

    print("\n📊 校验集统计：")
    for category, count in sorted(stats.items()):
        print(f"  {category}: {count} 个")

def main():
    excel_path = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/第5轮/24-11-副本.xlsx"
    src_dir = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/第5轮/文档下载"
    dst_dir = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/第5轮/文档整理"

    print("📌 开始读取 Excel 映射...")
    mapping = load_mapping(excel_path)

    print("📌 建立文件索引（递归多层目录，只处理 .xml 文件）...")
    file_index = index_files(src_dir, extensions=[".xml"])
    print(f"📌 共索引到 {len(file_index)} 个 .xml 文件")

    print("📌 复制全量文件...")
    patient_files = copy_all_files(dst_dir, file_index, mapping)

    print("📌 复制部分文件（每人每类最多10个）...")
    copy_limited_files(dst_dir, patient_files, mapping)

    print("📌 整理校验文件（按 SD-xx 分类，每类 ≤100）...")
    make_validation_set(dst_dir)

    print("✅ 文件整理完成！")


if __name__ == "__main__":
    main()
