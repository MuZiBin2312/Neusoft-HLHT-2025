import os

# 基础目录
base_dir = "/Users/lijiahe/Documents/Neusoft/proj/0800-互联互通"

# 新项目目录
new_project_dir = os.path.join(base_dir, "新项目")

# 一级文件夹列表
folders = [
    "0删节点备份",
    "0文档错误",
    "1文档下载",
    "2文档整理"
]

# 创建新项目目录（如果不存在）
os.makedirs(new_project_dir, exist_ok=True)

# 创建一级文件夹
for folder in folders:
    folder_path = os.path.join(new_project_dir, folder)
    os.makedirs(folder_path, exist_ok=True)
    print(f"已创建文件夹: {folder_path}")
