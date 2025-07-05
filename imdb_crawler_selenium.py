#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
@Project    ：IMDBCrawler
@File       ：imdb_crawler_selenium.py
@Author     ：IronmanJay
@Date       ：2025/7/2 20:53
@Describe   ：使用Selenium对IMDB网站的HTML页面进行爬取
"""

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
import time
import os
import random
import traceback

# 在全局定义根目录路径，这样所有函数都能访问
ROOT_DIR = os.getcwd()


def read_imdb_ids_from_file(filename="data.txt"):
    """从文件中读取IMDb ID列表"""
    # 使用相对于根目录的路径
    filepath = os.path.join(ROOT_DIR, filename)
    imdb_ids = []

    try:
        print(f"尝试从文件 {filepath} 读取IMDb ID列表...")
        with open(filepath, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()  # 移除首尾空白
                if line.startswith("tt") and len(line) >= 9:  # 基本验证
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
    """从文件中删除已成功处理的IMDb ID"""
    # 使用相对于根目录的路径
    filepath = os.path.join(ROOT_DIR, filename)

    try:
        print(f"正在从 {filepath} 中移除ID: {imdb_id}")
        # 读取所有行
        with open(filepath, "r", encoding="utf-8") as file:
            lines = file.readlines()

        # 移除包含该ID的行
        new_lines = [line for line in lines if line.strip() != imdb_id]

        # 写回文件
        with open(filepath, "w", encoding="utf-8") as file:
            file.writelines(new_lines)

        print(f"✅ 已从 {filepath} 中成功移除ID: {imdb_id}")
        return True
    except Exception as e:
        print(f"❌ 从 {filepath} 中移除ID失败: {imdb_id}, 原因: {str(e)}")
        traceback.print_exc()  # 打印错误详情
        return False


def save_imdb_html(imdb_id, driver):
    """安全地保存IMDb页面，带重试机制"""
    url = f"https://www.imdb.com/title/{imdb_id}/plotsummary/?ref_=tt_stry_pl"
    filename = f"{imdb_id}.html"

    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            print(f"尝试 [{imdb_id}] (尝试 {attempt + 1}/{max_retries + 1})")

            # 设置合理的页面加载超时
            driver.set_page_load_timeout(20)

            print(f"访问URL: {url}")
            driver.get(url)

            # 随机等待时间 (更长的范围)
            wait_time = random.uniform(3.0, 8.0)  # 3-8秒
            print(f"等待 {wait_time:.1f} 秒让页面完全加载...")
            time.sleep(wait_time)

            # 获取页面源码
            html = driver.page_source

            # 改进的验证方式
            if is_content_valid(html, imdb_id):
                # HTML文件保存在输出目录中
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"✅ 成功保存: {filename}")

                # 只有在成功保存后才从文件中删除ID
                if remove_id_from_file(imdb_id):
                    print(f"✅ 已从源文件中移除ID: {imdb_id}")
                else:
                    print(f"⚠ 警告: ID {imdb_id} 已成功处理但未能从源文件中移除")
                return True
            else:
                # 如果验证失败，先尝试刷新页面
                print("⚠ 内容验证失败，刷新页面...")
                driver.refresh()
                time.sleep(2)  # 给刷新后的页面加载时间
        except Exception as e:
            print(f"尝试处理 {imdb_id} 时出错: {str(e)}")
            print("异常类型:", type(e).__name__)

            # 如果是浏览器相关错误，直接返回失败
            if "session" in str(e).lower() or "renderer" in str(e).lower():
                print("浏览器会话错误，无法继续尝试")
                return False

            # 如果是连接错误，稍作等待后重试
            if "timeout" in str(e).lower() or "timed out" in str(e).lower():
                print("连接超时，等待后重试...")
                time.sleep(3)

    print(f"❌ 处理 {imdb_id} 最终失败")
    return False


def is_content_valid(html, imdb_id):
    """改进的内容验证，放宽要求"""
    # 基础长度检查
    if len(html) < 10000:  # 降低最小长度要求
        print(f"⚠ 内容过短: {len(html)} 字节")
        return False

    # 关键词检查
    keywords = ["imdb", imdb_id, "summary", "plot", "synopsis"]
    found = any(kw in html.lower() for kw in keywords)

    # 负向检查
    if "captcha" in html.lower() or "access denied" in html.lower():
        print("⚠ 检测到验证码或访问拒绝")
        return False

    # 允许部分缺失关键词
    if not found:
        print("⚠ 未找到所有关键词，但长度足够")
        return True  # 仅发出警告但允许保存

    return True


def create_driver():
    """创建WebDriver实例，添加稳定性增强"""
    print("正在启动浏览器实例...")

    options = webdriver.EdgeOptions()

    # 关键稳定性设置
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")  # 减少日志输出

    # 更自然的用户代理
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0")

    # 其他优化
    options.add_argument("--start-maximized")  # 最大化窗口
    options.add_experimental_option("excludeSwitches", ["enable-logging"])

    # 创建driver
    try:
        driver = webdriver.Edge(options=options)
        # 设置合理的初始超时
        driver.set_page_load_timeout(30)
        driver.implicitly_wait(5)
        print("✅ 浏览器已就绪")
        return driver
    except WebDriverException as e:
        print(f"❌ 创建浏览器实例失败: {str(e)}")
        print("请确保已正确安装Microsoft Edge WebDriver")
        print(f"下载地址: https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/")
        return None


def batch_save_imdb_html(id_list):
    """批量处理IMDb ID，更稳定的版本"""
    if not id_list:
        print("错误: 没有可处理的ID")
        return 0, []

    print(f"IMDb批量处理开始，共 {len(id_list)} 个ID")
    print("=" * 60)

    # 创建输出目录
    output_dir = "imdb_plots"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(ROOT_DIR, output_dir)
    print(f"输出目录: {output_path}")

    # 记录原始目录
    original_dir = os.getcwd()
    os.chdir(output_path)

    success_count = 0
    failed_ids = []

    # 启动浏览器
    driver = None
    try:
        driver = create_driver()
        if not driver:
            print("无法启动浏览器，终止程序")
            return 0, []

        for i, imdb_id in enumerate(id_list):
            print(f"\n[处理 {i + 1}/{len(id_list)}] ID: {imdb_id}")

            # 检查浏览器状态
            try:
                driver.current_url  # 简单的健康检查
            except:
                print("浏览器会话异常，尝试重启...")
                try:
                    driver.quit()
                except:
                    pass
                driver = create_driver()
                if not driver:
                    print("无法重启浏览器，终止剩余任务")
                    break

            # 处理当前ID
            if save_imdb_html(imdb_id, driver):
                success_count += 1
            else:
                failed_ids.append(imdb_id)

            # 添加更长、可变的延迟 (5-15秒)
            if i < len(id_list) - 1:
                delay = random.uniform(8.0, 15.0)
                print(f"等待 {delay:.1f} 秒后继续处理下一个...")
                time.sleep(delay)

    except Exception as e:
        print(f"批量处理严重错误: {str(e)}")
        traceback.print_exc()  # 打印完整堆栈
    finally:
        # 关闭浏览器
        if driver:
            try:
                driver.quit()
                print("✅ 浏览器已关闭")
            except Exception as e:
                print(f"❌ 关闭浏览器时出错: {str(e)}")

        # 返回原始工作目录
        os.chdir(original_dir)
        print(f"✅ 已返回原始工作目录: {original_dir}")

        return success_count, failed_ids


if __name__ == "__main__":
    # 打印根目录信息
    print(f"根目录: {ROOT_DIR}")
    print(f"当前工作目录: {os.getcwd()}")

    # 从文件读取IMDb ID列表
    imdb_ids = read_imdb_ids_from_file("data.txt")

    if not imdb_ids:
        print("没有有效的IMDb ID可处理，程序终止")
        print("\n按Enter键退出程序...")
        input()
        exit()

    # 执行批量处理
    print("启动IMDb批量处理...")
    success_count, failed_ids = batch_save_imdb_html(imdb_ids)

    # 结果报告
    print("\n" + "=" * 60)
    print(f"处理完成! 成功: {success_count}, 失败: {len(failed_ids)}")

    if failed_ids:
        print("\n以下ID处理失败:")
        for fid in failed_ids:
            print(f" - {fid}")

        # 创建失败列表文件 - 使用根目录路径
        failed_file = os.path.join(ROOT_DIR, "failed_ids.txt")
        with open(failed_file, "w") as f:
            f.write("\n".join(failed_ids))
        print(f"\n失败ID已保存到: {failed_file}")

    print("\n按Enter键退出程序...")
    input()