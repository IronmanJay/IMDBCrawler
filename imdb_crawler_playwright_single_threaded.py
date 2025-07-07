#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
@Project    ：IMDBCrawler
@File       ：imdb_crawler_playwright_single_threaded.py
@Author     ：IronmanJay
@Date       ：2025/7/4 17:21
@Describe   ：使用Playwright（单线程）对IMDB网站的HTML页面进行爬取
"""

import os
import time
import traceback
from playwright.sync_api import sync_playwright
import random


class IMDBCrawler:
    def __init__(self):
        self.ROOT_DIR = os.getcwd()
        self.IMDB_ID_FILE = "data.txt"
        self.OUTPUT_DIR = "debug_results"
        self.FAILED_FILE = "failed_ids.txt"
        self.TIMEOUT = 10000  # 页面加载超时(ms)
        self.RETRY_COUNT = 2  # 最大重试次数
        self.HEADLESS = True  # 是否无头浏览器
        self.browser = None
        self.context = None
        self.page = None

        # 设置浏览器启动参数
        self.browser_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--disable-gpu"
        ]

        # 设置浏览器headers
        self.browser_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.imdb.com/"
        }

    def read_imdb_ids_from_file(self, filename=None):
        """
        读取IMDB的ID文件
        :param filename: IMDB的ID文件
        :return: 读取内容
        """
        filename = filename or self.IMDB_ID_FILE
        filepath = os.path.join(self.ROOT_DIR, filename)
        imdb_ids = []
        try:
            print(f"🔍 尝试从文件 {filepath} 读取IMDb ID列表...")
            with open(filepath, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if line.startswith("tt") and len(line) >= 9:
                        imdb_ids.append(line)

            if not imdb_ids:
                raise ValueError("⚠️ 文件为空或未找到有效的IMDb ID")

            print(f"📋 从文件 {filepath} 读取了 {len(imdb_ids)} 个有效的IMDb ID")
            return imdb_ids
        except FileNotFoundError:
            print(f"❌ 错误: 文件 {filepath} 不存在")
            return []
        except Exception as e:
            print(f"❌ 读取文件时发生错误: {str(e)}")
            return []

    def remove_id_from_file(self, imdb_id, filename=None):
        filename = filename or self.IMDB_ID_FILE
        filepath = os.path.join(self.ROOT_DIR, filename)
        try:
            print(f"🗑️ 正在从 {filepath} 中移除ID: {imdb_id}")
            with open(filepath, "r", encoding="utf-8") as file:
                lines = file.readlines()

            new_lines = [line for line in lines if line.strip() != imdb_id]

            with open(filepath, "w", encoding="utf-8") as file:
                file.writelines(new_lines)

            print(f"✅ 已从 {filepath} 中成功移除ID: {imdb_id}")
            return True
        except Exception as e:
            print(f"❌ 从 {filepath} 中移除ID失败: {imdb_id}, 原因: {str(e)}")
            traceback.print_exc()
            return False

    def save_html(self, imdb_id):
        html_path = os.path.join(self.OUTPUT_DIR, f"{imdb_id}.html")
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        content = self.page.content()

        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)

        print(f"💾 [{imdb_id}] 已保存到: {html_path}")
        return html_path

    def is_challenge_page(self, html=None):
        if html is None:
            html = self.page.content()
        return "awswaf" in html.lower() or "challenge-container" in html.lower()

    def setup_browser(self):
        """
        初始化浏览器环境
        :return: None
        """
        print("🔧 正在初始化浏览器环境...")
        playwright = sync_playwright().start()
        self.browser = playwright.chromium.launch(
            headless=self.HEADLESS,
            args=self.browser_args
        )

        print("🧩 创建新浏览器上下文...")
        self.context = self.browser.new_context()

        # 拦截不必要资源，加快加载速度
        self.context.route("**/*", lambda route:
        route.abort() if route.request.resource_type in ["image", "font", "stylesheet"]
        else route.continue_())

        print("📄 创建新页面...")
        self.page = self.context.new_page()

        # 设置全局headers
        self.page.set_extra_http_headers(self.browser_headers)
        print("✅ 浏览器环境初始化完成")

    def close_browser(self):
        """
        关闭浏览器环境
        :return: None
        """
        if self.page:
            print("🛑 正在关闭页面...")
            self.page.close()
        if self.context:
            print("🛑 正在关闭浏览器上下文...")
            self.context.close()
        if self.browser:
            print("🛑 正在关闭浏览器...")
            self.browser.close()
        print("✅ 浏览器环境已关闭")

    def fetch_imdb_page(self, imdb_id):
        """
        抓取单个IMDB页面
        :param imdb_id: IMDB页面对应的ID
        :return: 抓取结果
        """
        url = f"https://www.imdb.com/title/{imdb_id}/plotsummary/"

        print(f"\n{'=' * 60}")
        print(f"🌐 [{imdb_id}] 开始处理")
        print(f"🔗 目标URL: {url}")
        print(f"🔄 最大重试次数: {self.RETRY_COUNT}")
        print('=' * 60)

        for attempt in range(1, self.RETRY_COUNT + 1):
            try:
                print(f"📡 [{imdb_id}] 第{attempt}次访问")
                self.page.goto(url, timeout=self.TIMEOUT)

                # 尝试检测目标元素
                try:
                    self.page.wait_for_selector("#summaries", timeout=5000)
                except Exception:
                    print(f"⚠️ [{imdb_id}] 剧情区块加载异常，将继续保存HTML")

                # 检查是否是验证页面
                if self.is_challenge_page():
                    print(f"⚠️ [{imdb_id}] 检测到挑战页面，刷新重试...")
                    self.page.reload(timeout=self.TIMEOUT)
                    self.page.wait_for_selector('div[data-testid="sub-section-summaries"]', timeout=8000)
                    if self.is_challenge_page():
                        raise Exception("⛔ 刷新后仍然是挑战页")

                return True
            except Exception as e:
                print(f"❌ [{imdb_id}] 尝试{attempt}失败: {str(e)}")
                if attempt < self.RETRY_COUNT:
                    wait_sec = 3 + attempt * 2
                    jitter = random.uniform(0.5, 2.5)  # 增加抖动防止节奏规律
                    total_wait = wait_sec + jitter
                    print(f"😴 访问失败，等待 {total_wait:.1f}秒后重试...")
                    time.sleep(total_wait)

        return False

    def fetch_all_sequential(self):
        """
        顺序抓取所有IMDB页面
        :return: 抓取结果
        """
        print("\n" + "=" * 60)
        print("🚀 开始顺序抓取所有IMDB页面")
        print("=" * 60)

        imdb_ids = self.read_imdb_ids_from_file()
        if not imdb_ids:
            print("⚠️ 无有效IMDb ID，退出")
            return []

        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        failed_ids = []
        total = len(imdb_ids)

        # 初始化浏览器环境
        self.setup_browser()

        start_time = time.time()

        for index, imdb_id in enumerate(imdb_ids):
            print(f"\n📥 处理进度: [{index + 1}/{total}] - ID: {imdb_id}")

            try:
                success = self.fetch_imdb_page(imdb_id)
                if success:
                    self.save_html(imdb_id)
                    self.remove_id_from_file(imdb_id)
                    print(f"✅ [{imdb_id}] 处理成功")
                else:
                    print(f"⛔ [{imdb_id}] 处理失败")
                    failed_ids.append(imdb_id)
            except Exception as e:
                print(f"🔥 处理 {imdb_id} 时发生未捕获异常: {e}")
                traceback.print_exc()
                failed_ids.append(imdb_id)

        # 关闭浏览器环境
        self.close_browser()

        elapsed_time = time.time() - start_time
        print(f"\n⏱️ 总耗时: {elapsed_time:.2f}秒")

        return failed_ids

    def run(self):
        """
        运行爬虫
        :return: None
        """
        print("=" * 60)
        print("🚀 IMDb批量HTML保存器（单进程顺序版）")
        print("=" * 60)

        imdb_ids = self.read_imdb_ids_from_file()
        if not imdb_ids:
            print("⚠️ 无有效IMDb ID，退出")
            return

        print(f"📝 准备处理 {len(imdb_ids)} 个IMDB ID")

        start = time.time()
        failed_ids = self.fetch_all_sequential()

        print("\n📊 处理结果统计:")
        print("=" * 30)
        print(f"📝 总计处理: {len(imdb_ids)}")
        print(f"✅ 成功数量: {len(imdb_ids) - len(failed_ids)}")
        print(f"❌ 失败数量: {len(failed_ids)}")
        print(f"⏱️ 用时: {int(time.time() - start)} 秒")
        print("=" * 30)

        if failed_ids:
            with open(self.FAILED_FILE, "w", encoding="utf-8") as f:
                f.write("\n".join(failed_ids))
            print(f"\n📁 失败ID已保存至: {self.FAILED_FILE}")

        print("\n🎉 所有任务完成！按Enter键退出...")
        input()


if __name__ == "__main__":
    crawler = IMDBCrawler()
    crawler.run()
