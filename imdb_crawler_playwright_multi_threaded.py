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

# ==== 配置项 ====
ROOT_DIR = os.getcwd()
IMDB_ID_FILE = "data_part2.txt"
OUTPUT_DIR = r"/Users/ironmanjay/data"  # 修改为你的目标路径
FAILED_FILE = "failed_ids.txt"
RETRY_COUNT = 2
CONCURRENCY = 6  # 最大并发数
TIMEOUT = 10000  # 页面加载超时

# ==== 工具函数 ====
def read_imdb_ids_from_file(filename=IMDB_ID_FILE):
    filepath = os.path.join(ROOT_DIR, filename)
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip().startswith("tt")]
    except Exception as e:
        print(f"读取 IMDb ID 失败: {e}")
        return []

def remove_id_from_file(imdb_id, filename=IMDB_ID_FILE):
    filepath = os.path.join(ROOT_DIR, filename)
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()
        new_lines = [line for line in lines if line.strip() != imdb_id]
        with open(filepath, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
    except Exception as e:
        print(f"移除 ID 失败: {imdb_id} - {e}")
        traceback.print_exc()

async def is_challenge_page(html: str):
    return "awswaf" in html.lower() or "challenge-container" in html.lower()

async def save_html(content: str, imdb_id: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{imdb_id}.html")

    def write_file():
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    await asyncio.to_thread(write_file)
    print(f"✅ [{imdb_id}] 已保存: {path}")


# ==== 抓取核心 ====
async def fetch_one(playwright, semaphore, imdb_id):
    url = f"https://www.imdb.com/title/{imdb_id}/plotsummary/"
    async with semaphore:
        try:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            await page.route("**/*", lambda route: route.abort() if route.request.resource_type in [
                "image", "stylesheet", "font"] else route.continue_())

            for attempt in range(1, RETRY_COUNT + 1):
                try:
                    await page.goto(url, timeout=30000, wait_until="domcontentloaded")
                    await page.wait_for_selector("#summaries", timeout=5000)
                    html = await page.content()
                    if await is_challenge_page(html):
                        await page.reload(timeout=TIMEOUT)
                        html = await page.content()
                        if await is_challenge_page(html):
                            raise Exception("仍为挑战页")
                    await save_html(html, imdb_id, OUTPUT_DIR)
                    remove_id_from_file(imdb_id)
                    await context.close()
                    await browser.close()
                    return None
                except Exception as e:
                    print(f"❌ [{imdb_id}] 第{attempt}次失败: {e}")
                    if attempt < RETRY_COUNT:
                        wait = 2 + attempt * 2 + random.uniform(0.5, 1.5)
                        print(f"⏳ 等待 {wait:.1f}s 后重试...")
                        await asyncio.sleep(wait)
            await context.close()
            await browser.close()
            return imdb_id
        except Exception as e:
            print(f"❌ [{imdb_id}] 爬取失败: {e}")
            return imdb_id

# ==== 主执行函数 ====
async def main():
    print("=" * 60)
    print("🚀 IMDb 多协程爬虫启动")
    print("=" * 60)

    imdb_ids = read_imdb_ids_from_file()
    if not imdb_ids:
        print("⚠️ 没有可处理的 ID，退出")
        return

    start = time.time()
    failed_ids = []

    semaphore = asyncio.Semaphore(CONCURRENCY)
    async with async_playwright() as playwright:
        tasks = [fetch_one(playwright, semaphore, imdb_id) for imdb_id in imdb_ids]
        results = await asyncio.gather(*tasks)
        failed_ids = [r for r in results if r]

    print("\n📊 总数: ", len(imdb_ids))
    print("✅ 成功: ", len(imdb_ids) - len(failed_ids))
    print("❌ 失败: ", len(failed_ids))
    print(f"⏱️ 总耗时: {int(time.time() - start)} 秒")

    if failed_ids:
        with open(FAILED_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(failed_ids))
        print(f"\n📁 失败ID已保存至: {FAILED_FILE}")

    input("\n🎉 完成！按Enter退出...")

# ==== 启动 ====
if __name__ == "__main__":
    asyncio.run(main())
