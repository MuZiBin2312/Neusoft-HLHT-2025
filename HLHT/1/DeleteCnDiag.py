# 删 中医证候诊断/编码
from lxml import etree
import os
import shutil

# 原始文件根目录（可能有子目录）
folder = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/模拟-11-18/1文档下载"
# 备份目录
backup_folder = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/模拟-11-18/删节点备份"
os.makedirs(backup_folder, exist_ok=True)

ns = {"hl7": "urn:hl7-org:v3"}

# 遍历目录及子目录
for root_dir, dirs, files in os.walk(folder):
    for filename in files:
        if not filename.endswith(".xml") or "SD-01" not in filename:
            continue

        file_path = os.path.join(root_dir, filename)
        print(f"\n处理文件：{file_path}")

        try:
            tree = etree.parse(file_path)
            root = tree.getroot()

            entries = root.xpath(".//hl7:entry", namespaces=ns)
            removed_count = 0

            for entry in entries:
                obs = entry.find("hl7:observation", namespaces=ns)
                if obs is None:
                    continue

                main_value = obs.find("hl7:value", namespaces=ns)
                main_code = main_value.get("code") if main_value is not None else None

                rel_obs_list = obs.findall("hl7:entryRelationship/hl7:observation", namespaces=ns)
                rel_code = None
                for rel_obs in rel_obs_list:
                    val = rel_obs.find("hl7:value", namespaces=ns)
                    if val is not None:
                        rel_code = val.get("code")
                        break

                if main_code == "A17." and rel_code == "B09.":
                    parent = entry.getparent()
                    parent.remove(entry)
                    removed_count += 1

            if removed_count > 0:
                # 备份原文件
                backup_path = os.path.join(backup_folder, filename)
                shutil.copy2(file_path, backup_path)
                print(f"  💾 已备份原文件到: {backup_path}")

                # 覆盖原文件
                tree.write(file_path, encoding="utf-8", xml_declaration=True, pretty_print=True)
                print(f"  ✅ 已删除 {removed_count} 个中医诊断 entry，并覆盖原文件")
            else:
                print("  ⚪ 无需删除（未发现中医诊断占位 entry）")

        except Exception as e:
            print(f"  ❌ 解析失败: {e}")
