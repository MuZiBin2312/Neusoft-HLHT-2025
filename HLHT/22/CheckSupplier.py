# 检查高值耗材中 供应商和生产厂家的完整性
import os
import xml.etree.ElementTree as ET
import re

folder_path = r"/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通/模拟-11-18/2文档整理/4.全量校验/SD-22"  # ← 改成你的路径

ok_count = 0
bad_count = 0
error_count = 0

for filename in os.listdir(folder_path):
    if not filename.lower().endswith(".xml"):
        continue

    file_path = os.path.join(folder_path, filename)

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # 🧩 自动提取命名空间前缀
        m = re.match(r'\{.*\}', root.tag)
        ns = m.group(0) if m else ''
        nsmap = {'ns': ns[1:-1]} if ns else {}

        # 🧭 查找 entry 节点（支持有命名空间的 XML）
        entries = root.findall(".//ns:entry", nsmap) if ns else root.findall(".//entry")

        if not entries:
            print(f"❌ {filename}：没有 <entry> 节点")
            bad_count += 1
            continue

        file_ok = False

        for entry in entries:
            material_name = (
                entry.findtext(".//ns:manufacturedMaterial/ns:name", "", nsmap)
                if ns
                else entry.findtext(".//manufacturedMaterial/name", "")
            ).strip()

            manu_nodes = (
                entry.findall(".//ns:manufacturerOrganization", nsmap)
                if ns
                else entry.findall(".//manufacturerOrganization")
            )

            for manu in manu_nodes:
                name1 = (
                    manu.findtext("ns:name", "", nsmap)
                    if ns
                    else manu.findtext("name", "")
                ).strip()
                name2 = (
                    manu.findtext("ns:asOrganizationPartOf/ns:wholeOrganization/ns:name", "", nsmap)
                    if ns
                    else manu.findtext("asOrganizationPartOf/wholeOrganization/name", "")
                ).strip()

                if name1 and name2:
                    print(f"✅ {filename}：OK → 耗材：{material_name}（{name1} / {name2}）")
                    file_ok = True
                    break

            if file_ok:
                break

        if file_ok:
            ok_count += 1
        else:
            print(f"❌ {filename}：所有 <entry> 中 manufacturerOrganization 节点都不完整")
            bad_count += 1

    except ET.ParseError:
        print(f"⚠️ {filename}：XML 格式错误（无法解析）")
        error_count += 1
    except Exception as e:
        print(f"⚠️ {filename}：处理出错：{e}")
        error_count += 1

print("\n" + "=" * 70)
print("📊 检查结果汇总：")
print(f"✅ 正常文件数：{ok_count}")
print(f"❌ 缺失或不完整文件数：{bad_count}")
print(f"⚠️ 解析错误文件数：{error_count}")
print(f"📁 总计检查文件数：{ok_count + bad_count + error_count}")
print("=" * 70)
