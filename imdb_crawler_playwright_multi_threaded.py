#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
@Project    ：IMDBCrawler
@File       ：imdb_crawler_playwright_multi_threaded.py
@Author     ：IronmanJay
@Date       ：2025/7/4 23:49
@Describe   ：使用Playwright（多线程）对IMDB网站的HTML页面进行爬取
"""

import os
import time
import random
import asyncio
import traceback
from playwright.async_api import async_playwright


class IMDBCrawler:
    def __init__(self):
        self.ROOT_DIR = os.getcwd()
        self.IMDB_ID_FILE = "data_part2.txt"
        self.OUTPUT_DIR = r"/Users/ironmanjay/data"
        self.FAILED_FILE = "failed_ids.txt"
        self.RETRY_COUNT = 2
        self.CONCURRENCY = 6
        self.TIMEOUT = 10000
        self.playwright = None
        self.browser = None

    def read_imdb_ids_from_file(self):
        """
        读取IMDB的ID文件
        :return: 读取内容
        """
        filepath = os.path.join(self.ROOT_DIR, self.IMDB_ID_FILE)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return [line.strip() for line in f if line.strip().startswith("tt")]
        except Exception as e:
            print(f"读取 IMDb ID 失败: {e}")
            return []

    def remove_id_from_file(self, imdb_id):
        """
        删除已经爬取完毕的ID
        :param imdb_id: IMDB的HTML页面对应的ID
        :return: 删除结果
        """
        filepath = os.path.join(self.ROOT_DIR, self.IMDB_ID_FILE)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                lines = f.readlines()
            new_lines = [line for line in lines if line.strip() != imdb_id]
            with open(filepath, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
        except Exception as e:
            print(f"移除 ID 失败: {imdb_id} - {e}")
            traceback.print_exc()

    async def is_challenge_page(self, html: str):
        """
        挑战页判断
        :param html: HTML页面
        :return: 是否是挑战页
        """
        return "awswaf" in html.lower() or "challenge-container" in html.lower()

    async def save_html(self, content: str, imdb_id: str):
        """
        保存爬取的HTML页面
        :param content: HTML页面内容
        :param imdb_id: IMDB的HTML页面ID
        :return: 保存结果
        """
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        path = os.path.join(self.OUTPUT_DIR, f"{imdb_id}.html")

        def write_file():
            """
            读取文件
            :return: 读取结果
            """
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)

        await asyncio.to_thread(write_file)
        print(f"✅ [{imdb_id}] 已保存: {path}")

    async def setup_browser(self):
        """
        设置浏览器
        :return: None
        """
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)

    async def close_browser(self):
        """
        关闭浏览器
        :return: None
        """
        await self.browser.close()
        await self.playwright.stop()

    async def create_context(self):
        """
        模拟用户请求
        :return: 模拟结果
        """
        return await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        )

    async def fetch_one(self, semaphore, imdb_id):
        """
        提取页面内容
        :param semaphore: 锁
        :param imdb_id: IMDB页面对应的ID
        :return: IMDB页面对应的ID
        """
        url = f"https://www.imdb.com/title/{imdb_id}/plotsummary/"
        async with semaphore:
            try:
                context = await self.create_context()
                page = await context.new_page()
                await page.route("**/*", lambda route: route.abort() if route.request.resource_type in [
                    "image", "stylesheet", "font"] else route.continue_())

                for attempt in range(1, self.RETRY_COUNT + 1):
                    try:
                        await page.goto(url, timeout=30000, wait_until="domcontentloaded")
                        await page.wait_for_selector("#summaries", timeout=5000)
                        html = await page.content()

                        if await self.is_challenge_page(html):
                            await page.reload(timeout=self.TIMEOUT)
                            html = await page.content()
                            if await self.is_challenge_page(html):
                                raise Exception("仍为挑战页")

                        await self.save_html(html, imdb_id)
                        self.remove_id_from_file(imdb_id)
                        await context.close()
                        return None
                    except Exception as e:
                        print(f"❌ [{imdb_id}] 第{attempt}次失败: {e}")
                        if attempt < self.RETRY_COUNT:
                            wait = 2 + attempt * 2 + random.uniform(0.5, 1.5)
                            print(f"⏳ 等待 {wait:.1f}s 后重试...")
                            await asyncio.sleep(wait)

                await context.close()
                return imdb_id
            except Exception as e:
                print(f"❌ [{imdb_id}] 爬取失败: {e}")
                return imdb_id

    async def main(self):
        """
        主函数
        :return: None
        """
        print("=" * 60)
        print("🚀 IMDb 多协程爬虫启动")
        print("=" * 60)

        imdb_ids = self.read_imdb_ids_from_file()
        if not imdb_ids:
            print("⚠️ 没有可处理的 ID，退出")
            return

        await self.setup_browser()
        start = time.time()

        semaphore = asyncio.Semaphore(self.CONCURRENCY)
        tasks = [self.fetch_one(semaphore, imdb_id) for imdb_id in imdb_ids]
        results = await asyncio.gather(*tasks)
        failed_ids = [r for r in results if r]

        await self.close_browser()

        print("\n📊 总数: ", len(imdb_ids))
        print("✅ 成功: ", len(imdb_ids) - len(failed_ids))
        print("❌ 失败: ", len(failed_ids))
        print(f"⏱️ 总耗时: {int(time.time() - start)} 秒")

        if failed_ids:
            with open(self.FAILED_FILE, "w", encoding="utf-8") as f:
                f.write("\n".join(failed_ids))
            print(f"\n📁 失败ID已保存至: {self.FAILED_FILE}")

        input("\n🎉 完成！按Enter退出...")


if __name__ == "__main__":
    crawler = IMDBCrawler()
    asyncio.run(crawler.main())
