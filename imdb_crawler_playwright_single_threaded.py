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

# ==== 配置项 ====
ROOT_DIR = os.getcwd()
IMDB_ID_FILE = "data.txt"
OUTPUT_DIR = "debug_results"
FAILED_FILE = "failed_ids.txt"
TIMEOUT = 10000                # 页面加载超时(ms)
RETRY_COUNT = 2                # 最大重试次数
HEADLESS = True               # 是否无头浏览器

# ==== 工具函数 ====
def read_imdb_ids_from_file(filename="data.txt"):
    filepath = os.path.join(ROOT_DIR, filename)
    imdb_ids = []
    try:
        print(f"尝试从文件 {filepath} 读取IMDb ID列表...")
        with open(filepath, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line.startswith("tt") and len(line) >= 9:
                    imdb_ids.append(line)
        if not imdb_ids:
            raise ValueError("文件为空或未找到有效的IMDb ID")
        print(f"从文件 {filepath} 读取了 {len(imdb_ids)} 个有效的IMDb ID")
        return imdb_ids
    except FileNotFoundError:
        print(f"错误: 文件 {filepath} 不存在")
        return []
    except Exception as e:
        print(f"读取文件时发生错误: {str(e)}")
        return []

def remove_id_from_file(imdb_id, filename="data.txt"):
    filepath = os.path.join(ROOT_DIR, filename)
    try:
        print(f"正在从 {filepath} 中移除ID: {imdb_id}")
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

def save_html(page, imdb_id, output_dir):
    html_path = os.path.join(output_dir, f"{imdb_id}.html")
    content = page.content()
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"✅ [{imdb_id}] 已保存到: {html_path}")

def is_challenge_page(html):
    return "awswaf" in html.lower() or "challenge-container" in html.lower()

def fetch_imdb_page(page, imdb_id):
    url = f"https://www.imdb.com/title/{imdb_id}/plotsummary/"

    page.set_extra_http_headers({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.imdb.com/"
    })

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            print(f"🌐 [{imdb_id}] 第{attempt}次访问: {url}")
            page.goto(url, timeout=TIMEOUT)

            try:
                page.wait_for_selector("#summaries", timeout=5000)
            except Exception:
                print(f"⚠️ [{imdb_id}] 剧情区块加载异常，继续保存HTML")

            html = page.content()

            if is_challenge_page(html):
                print(f"⚠️ [{imdb_id}] 检测到挑战页面，刷新重试...")
                page.reload(timeout=TIMEOUT)
                page.wait_for_selector('div[data-testid="sub-section-summaries"]', timeout=8000)
                html = page.content()
                if is_challenge_page(html):
                    raise Exception("仍然是挑战页")

            return True
        except Exception as e:
            print(f"❌ [{imdb_id}] 错误: {str(e)}")
            if attempt < RETRY_COUNT:
                wait_sec = 3 + attempt * 2
                jitter = random.uniform(0.5, 2.5)  # 增加抖动防止节奏规律
                total_wait = wait_sec + jitter
                print(f"😴 访问失败，等待 {total_wait:.1f}s 后重试...\n")
                time.sleep(total_wait)
            else:
                return False
    return False

# ==== 顺序爬取主逻辑 ====
def fetch_all_sequential(imdb_ids, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    failed_ids = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu"
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        )

        # 拦截不必要资源，加快加载速度
        context.route("**/*", lambda route, request:
            route.abort() if request.resource_type in ["image", "font", "stylesheet"]
            else route.continue_())

        # ✅ 只创建一个 page
        page = context.new_page()

        # ✅ 设置一次 headers，避免重复设置
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.imdb.com/"
        })

        total = len(imdb_ids)
        for index, imdb_id in enumerate(imdb_ids):
            print(f"📥 正在处理第 {index + 1}/{total} 个: {imdb_id}")
            try:
                success = fetch_imdb_page(page, imdb_id)
                if success:
                    save_html(page, imdb_id, output_dir)
                    remove_id_from_file(imdb_id)
                else:
                    failed_ids.append(imdb_id)
            except Exception as e:
                print(f"❌ 处理 {imdb_id} 时发生异常: {e}")

        # ✅ 所有处理完后统一关闭资源
        page.close()
        context.close()
        browser.close()

    return failed_ids



# ==== 主入口 ====
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 IMDb批量HTML保存器（单进程顺序版）")
    print("=" * 60)

    imdb_ids = read_imdb_ids_from_file(IMDB_ID_FILE)
    if not imdb_ids:
        print("⚠️ 无有效IMDb ID，退出")
        exit()

    start = time.time()
    failed_ids = fetch_all_sequential(imdb_ids, OUTPUT_DIR)

    print("\n📊 总计处理: ", len(imdb_ids))
    print("✅ 成功数量: ", len(imdb_ids) - len(failed_ids))
    print("❌ 失败数量: ", len(failed_ids))
    print(f"⏱️ 用时: {int(time.time() - start)} 秒")

    if failed_ids:
        with open(FAILED_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(failed_ids))
        print(f"\n📁 失败ID已保存至: {FAILED_FILE}")

    print("\n🎉 所有任务完成！按Enter键退出...")
    input()
