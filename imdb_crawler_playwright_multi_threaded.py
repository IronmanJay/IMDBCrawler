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
import traceback
import random
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from playwright.sync_api import sync_playwright

# ==== 配置项 ====
ROOT_DIR = os.getcwd()
IMDB_ID_FILE = "data.txt"
OUTPUT_DIR = "debug_results"
FAILED_FILE = "failed_ids.txt"
TIMEOUT = 10000
RETRY_COUNT = 2
HEADLESS = True
MAX_WORKERS = 4

# ==== 工具函数 ====
def read_imdb_ids_from_file(filename="data.txt"):
    filepath = os.path.join(ROOT_DIR, filename)
    imdb_ids = []
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line.startswith("tt") and len(line) >= 9:
                    imdb_ids.append(line)
        return imdb_ids
    except Exception as e:
        print(f"读取IMDb ID失败: {e}")
        return []

def remove_id_from_file(imdb_id, filename="data.txt"):
    filepath = os.path.join(ROOT_DIR, filename)
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            lines = file.readlines()
        new_lines = [line for line in lines if line.strip() != imdb_id]
        with open(filepath, "w", encoding="utf-8") as file:
            file.writelines(new_lines)
    except Exception as e:
        print(f"移除ID失败: {imdb_id} - {e}")
        traceback.print_exc()

def save_html(page, imdb_id, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{imdb_id}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(page.content())
    print(f"✅ [{imdb_id}] 已保存: {path}")

def is_challenge_page(html):
    return "awswaf" in html.lower() or "challenge-container" in html.lower()

def fetch_imdb_page(page, imdb_id):
    url = f"https://www.imdb.com/title/{imdb_id}/plotsummary/"
    page.set_extra_http_headers({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.imdb.com/"
    })

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            page.goto(url, timeout=TIMEOUT)
            page.wait_for_selector("#summaries", timeout=5000)
            html = page.content()
            if is_challenge_page(html):
                page.reload(timeout=TIMEOUT)
                html = page.content()
                if is_challenge_page(html):
                    raise Exception("仍为挑战页")
            return True
        except Exception as e:
            print(f"❌ [{imdb_id}] 第{attempt}次失败: {e}")
            if attempt < RETRY_COUNT:
                wait = 2 + attempt * 2 + random.uniform(0.5, 1.5)
                print(f"⏳ 等待 {wait:.1f}s 后重试...")
                time.sleep(wait)
    return False

# ==== 多线程共享Context执行逻辑 ====
def fetch_all_parallel(imdb_ids, output_dir):
    failed_ids = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

        # 禁用不必要的资源
        context.route("**/*", lambda route, request:
            route.abort() if request.resource_type in ["image", "font", "stylesheet"] else route.continue_()
        )

        lock = Lock()  # 控制日志或共享资源访问

        def worker(index_imdbid):
            index, imdb_id = index_imdbid
            try:
                with lock:
                    print(f"📥 正在处理 {index + 1}/{len(imdb_ids)}: {imdb_id}")
                page = context.new_page()
                success = fetch_imdb_page(page, imdb_id)
                if success:
                    save_html(page, imdb_id, output_dir)
                    remove_id_from_file(imdb_id)
                else:
                    failed_ids.append(imdb_id)
            except Exception as e:
                print(f"❌ 异常: {imdb_id} - {e}")
                failed_ids.append(imdb_id)
            finally:
                try:
                    page.close()
                except:
                    pass

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(worker, enumerate(imdb_ids))

        context.close()
        browser.close()

    return failed_ids

# ==== 主函数 ====
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 IMDb 多线程爬虫启动")
    print("=" * 60)

    imdb_ids = read_imdb_ids_from_file(IMDB_ID_FILE)
    if not imdb_ids:
        print("⚠️ 没有可处理的ID，程序退出")
        exit()

    start = time.time()
    failed = fetch_all_parallel(imdb_ids, OUTPUT_DIR)

    print("\n📊 总数: ", len(imdb_ids))
    print("✅ 成功: ", len(imdb_ids) - len(failed))
    print("❌ 失败: ", len(failed))
    print(f"⏱️ 总耗时: {int(time.time() - start)} 秒")

    if failed:
        with open(FAILED_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))
        print(f"\n📁 失败ID已写入: {FAILED_FILE}")

    input("\n🎉 完成！按Enter退出...")
