# 删 处方类别
from lxml import etree
import os
import shutil

# 原始文件根目录（可能有子目录）
folder = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/模拟10-29/1文档下载"
# 备份目录
backup_folder = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/模拟10-29/删节点备份"
os.makedirs(backup_folder, exist_ok=True)

ns = {"hl7": "urn:hl7-org:v3"}

# 遍历目录及子目录
for root_dir, dirs, files in os.walk(folder):
    for filename in files:
        if not filename.endswith(".xml") or "SD-05" not in filename:
            continue

        file_path = os.path.join(root_dir, filename)
        print(f"\n处理文件：{file_path}")

        try:
            tree = etree.parse(file_path)
            root = tree.getroot()

            # 查找所有的 <entry> 节点
            entries = root.xpath(".//hl7:entry", namespaces=ns)
            removed_count = 0

            for entry in entries:
                # 查找 entry 内的 observation 和其 value 节点
                template_id = entry.find(".//hl7:templateId", namespaces=ns)
                if template_id is not None and template_id.get("root") == "2.999.1.38" and template_id.get("extension") == "91205":
                    observation = entry.find(".//hl7:observation", namespaces=ns)
                    if observation is not None:
                        code = observation.find(".//hl7:code", namespaces=ns)
                        if code is not None and code.get("code") == "DE08.50.032.00":
                            value = observation.find(".//hl7:value", namespaces=ns)
                            if value is not None and value.get("code") == "1":
                                # 找到匹配的节点，删除这个 entry
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
                print(f"  ✅ 已删除 {removed_count} 个匹配的 entry，并覆盖原文件")
            else:
                print("  ⚪ 无需删除（未发现匹配的 entry）")

        except Exception as e:
            print(f"  ❌ 解析失败: {e}")
