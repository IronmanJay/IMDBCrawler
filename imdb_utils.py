#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
@Project    ：IMDBCrawler
@File       ：imdb_utils.py
@Author     ：IronmanJay
@Date       ：2025/7/5 14:02
@Describe   ：一些辅助工具
"""

import os


class IMDbDataCleaner:
    def __init__(self, data_file="data.txt", html_dir=r"D:\imdb_plots"):
        self.data_file = data_file
        self.html_dir = html_dir

    def load_html_ids(self):
        html_ids = set()
        if not os.path.exists(self.html_dir):
            print(f"❌ HTML目录不存在: {self.html_dir}")
            return html_ids

        for fname in os.listdir(self.html_dir):
            if fname.lower().endswith(".html") and fname.startswith("tt"):
                html_id = os.path.splitext(fname)[0]
                html_ids.add(html_id)
        print(f"📁 从HTML目录中提取到 {len(html_ids)} 个ID")
        return html_ids

    def clean_data_file(self):
        if not os.path.exists(self.data_file):
            print(f"❌ data.txt 文件不存在: {self.data_file}")
            return

        with open(self.data_file, "r", encoding="utf-8") as f:
            original_ids = [line.strip() for line in f if line.strip()]
        print(f"📄 原始data.txt中共有 {len(original_ids)} 个ID")

        html_ids = self.load_html_ids()

        remaining_ids = [id_ for id_ in original_ids if id_ not in html_ids]

        with open(self.data_file, "w", encoding="utf-8") as f:
            f.write("\n".join(remaining_ids) + ("\n" if remaining_ids else ""))
        print(f"✅ 清洗完成，剩余 {len(remaining_ids)} 个ID写回 data.txt")

    def run(self):
        print("=" * 60)
        print("🔧 IMDb ID 清洗工具启动")
        print("=" * 60)
        self.clean_data_file()
        print("🎉 清洗任务完成")


class IMDbDataSplitter:
    def __init__(self, data_file="data.txt", output_dir="."):
        self.data_file = data_file
        self.output_dir = output_dir

    def split_data_file(self):
        if not os.path.exists(self.data_file):
            print(f"❌ data.txt 文件不存在: {self.data_file}")
            return

        with open(self.data_file, "r", encoding="utf-8") as f:
            ids = [line.strip() for line in f if line.strip()]

        total = len(ids)
        if total == 0:
            print("⚠️ data.txt 为空，无法分割")
            return

        mid = total // 2
        part1 = ids[:mid]
        part2 = ids[mid:]

        part1_path = os.path.join(self.output_dir, "data_part1.txt")
        part2_path = os.path.join(self.output_dir, "data_part2.txt")

        with open(part1_path, "w", encoding="utf-8") as f1:
            f1.write("\n".join(part1) + "\n")
        with open(part2_path, "w", encoding="utf-8") as f2:
            f2.write("\n".join(part2) + "\n")

        print(f"✅ 分割完成，共 {total} 个ID：")
        print(f"    📄 写入 {len(part1)} 行到 {part1_path}")
        print(f"    📄 写入 {len(part2)} 行到 {part2_path}")

    def run(self):
        print("=" * 60)
        print("✂️ IMDb ID 分割工具启动")
        print("=" * 60)
        self.split_data_file()
        print("🎉 分割任务完成")


if __name__ == "__main__":
    state = "split"

    if state == "clean":
        # Step 1: 清洗 data.txt
        cleaner = IMDbDataCleaner(
            data_file="data.txt",
            html_dir=r"D:\imdb_plots"
        )
        cleaner.run()
    elif state == "split":
        # Step 2: 分割 data.txt
        splitter = IMDbDataSplitter(
            data_file="data.txt",
            output_dir="."  # 当前目录
        )
        splitter.run()
