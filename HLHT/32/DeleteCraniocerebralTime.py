# delete_sd32_coma_time.py
from lxml import etree
import os
import shutil

# 原始文件根目录（含子目录）
folder = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/正式-12-18/1文档下载"
# 备份目录
backup_folder = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/正式-12-18/0删节点备份/32"
os.makedirs(backup_folder, exist_ok=True)

ns = {"hl7": "urn:hl7-org:v3"}

# 需要排除（不做删除操作）的患者住院号
EXCLUDE_PATIENTS = [
    "ZY010000737755",
]

# 判断 entry 是否为 SD-32 的 “颅脑损伤患者昏迷时间”
def is_target_entry(entry):
    organizer = entry.find("hl7:organizer", namespaces=ns)
    if organizer is None:
        return False

    code_node = organizer.find("hl7:code", namespaces=ns)
    if code_node is None:
        return False

    disp = code_node.get("displayName", "")

    target_names = [
        "颅脑损伤患者入院前昏迷时间",
        "颅脑损伤患者入院后昏迷时间",
    ]

    if disp not in target_names:
        return False

    components = organizer.findall("hl7:component", namespaces=ns)
    for comp in components:
        obs = comp.find("hl7:observation", namespaces=ns)
        if obs is None:
            return False
        value = obs.find("hl7:value", namespaces=ns)
        if value is None:
            return False

        # 任意一个 value 不是 0 → 不删除
        if value.get("value") != "0":
            return False

    return True


# 遍历目录及子目录
for root_dir, dirs, files in os.walk(folder):
    for filename in files:

        if not filename.endswith(".xml") or "SD-32" not in filename:
            continue

        file_path = os.path.join(root_dir, filename)
        print(f"\n处理文件：{file_path}")

        try:
            tree = etree.parse(file_path)
            root = tree.getroot()

            # 读取患者号 <id extension="ZYxxxxxx">
            id_node = root.find(".//hl7:id[@root='2.16.156.10011.1.1']", namespaces=ns)
            inpatient_no = id_node.get("extension") if id_node is not None else None

            if inpatient_no in EXCLUDE_PATIENTS:
                print(f"  ⚠ 排除患者 {inpatient_no} → 跳过此文件")
                continue

            entries = root.xpath(".//hl7:entry", namespaces=ns)
            removed_count = 0

            for entry in entries:
                if is_target_entry(entry):
                    parent = entry.getparent()
                    parent.remove(entry)
                    removed_count += 1

            if removed_count > 0:
                backup_path = os.path.join(backup_folder, filename)
                shutil.copy2(file_path, backup_path)
                print(f"  💾 已备份到: {backup_path}")

                tree.write(file_path, encoding="utf-8", xml_declaration=True, pretty_print=True)
                print(f"  ✅ 删除 {removed_count} 个昏迷时间 entry（患者 {inpatient_no}）")
            else:
                print(f"  ⚪ 无需删除（患者 {inpatient_no}）")

        except Exception as e:
            print(f"  ❌ 解析失败: {e}")