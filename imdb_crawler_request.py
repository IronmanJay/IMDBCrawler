#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
@Project    ：IMDBCrawler
@File       ：imdb_crawler_request.py
@Author     ：IronmanJay
@Date       ：2025/7/5 0:46
@Describe   ：使用Request对IMDB网站的HTML页面进行爬取
"""

import os
import time
import random
import requests
import threading
import concurrent.futures


class IMDBCrawler:
    def __init__(self, imdb_file, output_dir, failed_file, timeout, retry, max_workers, cookie_str):
        self.root_dir = os.getcwd()
        self.imdb_file = imdb_file
        self.output_dir = output_dir
        self.failed_file = failed_file
        self.timeout = timeout
        self.retry = retry
        self.max_workers = max_workers
        self.cookie_str = cookie_str
        self.lock = threading.Lock()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:91.0) Gecko/20100101 Firefox/91.0",
        ]

    def read_ids(self):
        path = os.path.join(self.root_dir, self.imdb_file)
        try:
            with open(path, "r", encoding="utf-8") as f:
                ids = [line.strip() for line in f if line.strip().startswith("tt")]
            print(f"📖 成功读取 {len(ids)} 个 IMDb ID")
            return ids
        except Exception as e:
            print(f"❌ 读取 IMDb ID 失败: {e}")
            return []

    def remove_id(self, imdb_id):
        path = os.path.join(self.root_dir, self.imdb_file)
        try:
            # 加锁，确保同一时刻只有一个线程修改文件
            with self.lock:
                with open(path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                new_lines = [line for line in lines if line.strip() != imdb_id]
                with open(path, "w", encoding="utf-8") as f:
                    f.writelines(new_lines)
            print(f"🗑️ 已移除已完成 ID: {imdb_id}")
        except Exception as e:
            print(f"❌ 移除 ID 失败: {imdb_id}, 原因: {e}")

    def save_html(self, html, imdb_id):
        os.makedirs(self.output_dir, exist_ok=True)
        path = os.path.join(self.output_dir, f"{imdb_id}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"✅ [{imdb_id}] HTML 保存成功")

    def is_challenge_page(self, html):
        lower = html.lower()
        return "awswaf" in lower or "challenge-container" in lower or "captcha" in lower

    def fetch_page(self, imdb_id):
        session = requests.Session()
        session.cookies.set("at-main", self.cookie_str, domain=".imdb.com")
        url = f"https://www.imdb.com/title/{imdb_id}/plotsummary/"
        for attempt in range(1, self.retry + 1):
            try:
                headers = {
                    "User-Agent": random.choice(self.user_agents),
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.imdb.com/",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                }
                print(f"🌐 请求 {imdb_id} 第{attempt}次")
                res = session.get(url, headers=headers, timeout=self.timeout)
                if res.status_code != 200:
                    raise Exception(f"HTTP状态码异常: {res.status_code}")
                if self.is_challenge_page(res.text):
                    raise Exception("页面疑似反爬挑战")
                return res.text
            except Exception as e:
                print(f"⚠️ 第{attempt}次请求失败: {e}")
                time.sleep(3 + attempt * 2 + random.uniform(0.5, 2.5))
        return None

    def worker(self, imdb_id):
        try:
            html = self.fetch_page(imdb_id)
            if html:
                try:
                    self.save_html(html, imdb_id)
                    self.remove_id(imdb_id)
                    return None
                except Exception as se:
                    print(f"❌ [{imdb_id}] 保存失败: {se}")
                    return imdb_id
            else:
                return imdb_id
        except Exception as e:
            print(f"❌ [{imdb_id}] 爬取异常: {e}")
            return imdb_id

    def run(self):
        print("=" * 60)
        print("🚀 IMDb 多线程爬虫启动")
        print("=" * 60)
        ids = self.read_ids()
        if not ids:
            print("⚠️ 无可用 ID，退出")
            return
        start = time.time()
        failed = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.worker, imdb_id) for imdb_id in ids]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    failed.append(result)

        print("\n📊 总计: ", len(ids))
        print("✅ 成功: ", len(ids) - len(failed))
        print("❌ 失败: ", len(failed))
        print(f"⏱️ 耗时: {int(time.time() - start)} 秒")

        if failed:
            with open(self.failed_file, "w", encoding="utf-8") as f:
                f.write("\n".join(failed))
            print(f"📁 失败 ID 已保存到: {self.failed_file}")

        input("\n🎉 所有任务完成！按 Enter 退出...")


if __name__ == "__main__":
    crawler = IMDBCrawler(
        imdb_file="data.txt",
        output_dir=r"D:\debug_results",
        failed_file="failed_ids.txt",
        timeout=30,
        retry=5,
        max_workers=12,
        cookie_str="Atza|IwEBIMNFteiiyVjsJHpGqOhzM1PEZmU9gShL7_9gUBMZwB7K67tEMEGve4EQCeX-An2_vyoizO-PptAQhFAlsGlFEN7LXqHj0qLuObnOi1AuHe4sVxoCiOPDgJaDXa-CSlaa0R0WIINNZ6SNoyqWMr7IkvNTXNrQfbFvUziB9ckpy8MxFBHgQufYwOiF9_ZwsJClq1xidf8ipS9RUwONeF3jA31fbJ9KGPW2QNFN_qyXQy75qQ"
    )
    crawler.run()
