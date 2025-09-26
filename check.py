import os
import pandas as pd


def load_patient_ids_from_excel(excel_path: str) -> pd.DataFrame:
    """从 Excel 读取患者号，同时保留行号"""
    df = pd.read_excel(excel_path, dtype=str)
    df = df.fillna("")
    df["行号"] = df.index + 2  # Excel 行号（+2 是因为 index 从 0 开始，第一行是表头）
    return df


def load_patient_ids_from_full(full_dir: str) -> set:
    """从全量目录读取患者号集合（文件夹名格式：患者号-姓名）"""
    patient_ids = set()
    for folder in os.listdir(full_dir):
        if os.path.isdir(os.path.join(full_dir, folder)):
            parts = folder.split("-", 1)
            if parts:
                patient_ids.add(parts[0].strip())
    return patient_ids


def check_missing_patients(df: pd.DataFrame, full_ids: set):
    """比对并输出缺失患者（带 Excel 行号）"""
    excel_ids = set(df["住院流水号"].unique())
    missing = excel_ids - full_ids
    extra = full_ids - excel_ids

    print("\n📊 缺失排查结果：")
    print(f"Excel 共 {len(df)} 行数据")
    print(f"Excel 去重后 {len(excel_ids)} 个患者")
    print(f"全量目录 {len(full_ids)} 个患者")
    print(f"缺失 {len(missing)} 个患者")

    if missing:
        print("⚠️ 缺失的患者号及所在 Excel 行号：")
        miss_df = df[df["住院流水号"].isin(missing)]
        for _, row in miss_df.iterrows():
            print(f"  行 {row['行号']} -> {row['住院流水号']} {row.get('患者姓名', '')}")

    if extra:
        print(f"\nℹ️ 全量中多出的 {len(extra)} 个患者（Excel 没有）：")
        for pid in sorted(extra):
            print(f"  {pid}")


def main():
    excel_path = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/第4轮/患者列表24-10.xlsx"
    full_dir = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/第4轮/文档整理/1.全量"

    print("📌 开始读取 Excel 患者号...")
    df = load_patient_ids_from_excel(excel_path)

    print("📌 扫描全量目录患者号...")
    full_ids = load_patient_ids_from_full(full_dir)

    check_missing_patients(df, full_ids)


if __name__ == "__main__":
    main()