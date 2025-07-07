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


class IMDBCrawler:
    def __init__(self):
        self.ROOT_DIR = os.getcwd()
        self.IMDB_ID_FILE = "data.txt"
        self.OUTPUT_DIR = "imdb_plots"
        self.FAILED_FILE = "failed_ids.txt"

        # 配置选项
        self.headless = False                   # 是否使用无头模式
        self.timeout = 30                       # 页面加载超时时间（秒）
        self.retries = 2                        # 重试次数
        self.delay_range = (8.0, 15.0)          # 请求之间的随机延迟范围（秒）

        # 浏览器参数
        self.browser_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-gpu",
            "--log-level=3",                    # 减少日志输出
            "--start-maximized"
        ]
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"

        # 状态变量
        self.driver = None
        self.original_dir = self.ROOT_DIR

    def read_imdb_ids_from_file(self, filename=None):
        """
        从文件中读取IMDB页面对应的ID列表
        :param filename:
        :return:
        """
        filename = filename or self.IMDB_ID_FILE
        filepath = os.path.join(self.ROOT_DIR, filename)
        imdb_ids = []

        try:
            print(f"📖 尝试从文件 {filepath} 读取IMDb ID列表...")
            with open(filepath, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if line.startswith("tt") and len(line) >= 9:
                        imdb_ids.append(line)

            if not imdb_ids:
                raise ValueError("⚠️ 文件为空或未找到有效的IMDb ID")

            print(f"✅ 从文件 {filepath} 读取了 {len(imdb_ids)} 个有效的IMDb ID")
            return imdb_ids
        except FileNotFoundError:
            print(f"❌ 错误: 文件 {filepath} 不存在")
            return []
        except Exception as e:
            print(f"❌ 读取文件时发生错误: {str(e)}")
            traceback.print_exc()
            return []

    def remove_id_from_file(self, imdb_id, filename=None):
        """
        从文件中删除已成功处理的IMDB页面对应的ID
        :param imdb_id: IMDB页面对应的ID
        :param filename: 目标文件
        :return: 删除结果
        """
        filename = filename or self.IMDB_ID_FILE
        filepath = os.path.join(self.ROOT_DIR, filename)

        try:
            print(f"🗑️ 正在从 {filepath} 中移除ID: {imdb_id}")
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
            traceback.print_exc()
            return False

    def is_content_valid(self, html, imdb_id):
        """
        验证获取的HTML内容是否有效
        :param html: 目标HTML页面
        :param imdb_id: IMDB页面对应的ID
        :return: 判断结果
        """
        # 基础长度检查
        if len(html) < 10000:
            print(f"⚠️ 内容过短: {len(html)} 字节")
            return False

        # 关键词检查
        keywords = ["imdb", imdb_id, "summary", "plot", "synopsis"]
        found = any(kw in html.lower() for kw in keywords)

        # 负向检查
        if "captcha" in html.lower() or "access denied" in html.lower():
            print("⚠️ 检测到验证码或访问拒绝")
            return False

        # 允许部分缺失关键词
        if not found:
            print("⚠️ 未找到所有关键词，但长度足够")
            return True  # 仅发出警告但允许保存

        return True

    def create_driver(self):
        """
        创建WebDriver实例
        :return: 创建结果
        """
        print("🔧 正在启动浏览器实例...")

        try:
            options = webdriver.EdgeOptions()

            # 设置参数
            for arg in self.browser_args:
                options.add_argument(arg)

            if self.headless:
                options.add_argument("--headless=new")  # 无头模式

            # 设置用户代理
            options.add_argument(f"user-agent={self.user_agent}")

            # 创建driver
            self.driver = webdriver.Edge(options=options)

            # 设置超时
            self.driver.set_page_load_timeout(self.timeout)
            self.driver.implicitly_wait(5)

            print("✅ 浏览器实例已成功创建")
            return True
        except WebDriverException as e:
            print(f"❌ 创建浏览器实例失败: {str(e)}")
            print("请确保已正确安装Microsoft Edge WebDriver")
            print(f"下载地址: https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/")
            return False

    def init_browser(self):
        """
        初始化浏览器环境
        :return: 初始化结果
        """
        if not self.create_driver():
            return False

        print("🔧 正在切换到输出目录...")
        # 创建输出目录
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(self.ROOT_DIR, self.OUTPUT_DIR)
        print(f"📂 输出目录: {output_path}")

        # 记录原始目录
        self.original_dir = os.getcwd()
        os.chdir(output_path)

        return True

    def restart_browser(self):
        """
        重启浏览器实例
        :return: 重启结果
        """
        print("🔄 正在重启浏览器实例...")
        try:
            # 先尝试关闭现有浏览器
            if self.driver:
                self.driver.quit()
                print("✅ 浏览器已关闭")

            # 重新创建浏览器
            return self.create_driver()
        except Exception as e:
            print(f"❌ 重启浏览器失败: {str(e)}")
            return False

    def save_imdb_html(self, imdb_id):
        """
        保存IMDB页面
        :param imdb_id: IMDB页面对应的ID
        :return: 保存结果
        """
        url = f"https://www.imdb.com/title/{imdb_id}/plotsummary/"
        filename = f"{imdb_id}.html"

        print(f"\n{'=' * 60}")
        print(f"🌐 [{imdb_id}] 开始处理")
        print(f"🔗 URL: {url}")
        print(f"🔄 最大重试次数: {self.retries}")
        print('=' * 60)

        # 尝试次数
        for attempt in range(self.retries + 1):
            try:
                print(f"📡 尝试 #{attempt + 1}/{self.retries + 1}: {url}")

                # 设置超时
                self.driver.set_page_load_timeout(self.timeout)

                # 访问URL
                self.driver.get(url)

                # 随机等待时间
                wait_time = random.uniform(3.0, 8.0)  # 3-8秒
                print(f"⏳ 等待 {wait_time:.1f} 秒让页面完全加载...")
                time.sleep(wait_time)

                # 获取页面源码
                html = self.driver.page_source

                # 验证内容
                if self.is_content_valid(html, imdb_id):
                    # 保存HTML文件
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(html)
                    print(f"✅ HTML内容已保存: {filename}")

                    # 从源文件中移除ID
                    if self.remove_id_from_file(imdb_id):
                        print(f"✅ ID {imdb_id} 已从源文件中移除")
                    else:
                        print(f"⚠️ 警告: ID {imdb_id} 已成功处理但未能从源文件中移除")
                    return True
                else:
                    print("🔄 内容验证失败，刷新页面...")
                    self.driver.refresh()
                    time.sleep(3)
            except Exception as e:
                print(f"❌ 尝试 #{attempt + 1} 失败: {str(e)}")
                print(f"异常类型: {type(e).__name__}")

                # 检查浏览器会话是否有效
                try:
                    self.driver.current_url
                except:
                    print("⚠️ 浏览器会话异常")
                    if not self.restart_browser():
                        return False

                # 等待后重试
                if attempt < self.retries:
                    wait_time = random.uniform(2.0, 5.0)
                    print(f"⏳ 等待 {wait_time:.1f} 秒后重试...")
                    time.sleep(wait_time)

        print(f"❌ [{imdb_id}] 最终处理失败")
        return False

    def batch_process(self, imdb_ids):
        """
        批量处理IMDB页面对应的ID
        :param imdb_ids: IMDB页面对应的ID
        :return: 批量处理结果
        """
        if not imdb_ids:
            print("⚠️ 错误: 没有可处理的ID")
            return 0, [], False

        print(f"🚀 IMDb批量处理开始，共 {len(imdb_ids)} 个ID")
        print("=" * 60)

        # 初始化浏览器环境
        if not self.init_browser():
            print("❌ 无法初始化浏览器环境，程序终止")
            return 0, [], False

        success_count = 0
        failed_ids = []
        result = False

        try:
            for i, imdb_id in enumerate(imdb_ids):
                print(f"\n📥 处理进度: [{i + 1}/{len(imdb_ids)}] - ID: {imdb_id}")

                # 检查浏览器状态
                try:
                    self.driver.current_url  # 健康检查
                except:
                    print("⚠️ 浏览器会话异常，尝试重启...")
                    if not self.restart_browser():
                        print("❌ 无法重启浏览器，终止剩余任务")
                        break

                # 处理当前ID
                if self.save_imdb_html(imdb_id):
                    success_count += 1
                else:
                    failed_ids.append(imdb_id)

                # 请求之间的延迟
                if i < len(imdb_ids) - 1:
                    delay = random.uniform(*self.delay_range)
                    print(f"⏳ 等待 {delay:.1f} 秒后继续处理下一个...")
                    time.sleep(delay)

            result = True
        except Exception as e:
            print(f"❌ 批量处理严重错误: {str(e)}")
            traceback.print_exc()
        finally:
            # 关闭浏览器
            if self.driver:
                try:
                    self.driver.quit()
                    print("✅ 浏览器已关闭")
                except Exception as e:
                    print(f"❌ 关闭浏览器时出错: {str(e)}")

            # 返回原始工作目录
            os.chdir(self.original_dir)
            print(f"✅ 已返回原始工作目录: {self.original_dir}")

            return success_count, failed_ids, result

    def run(self):
        """
        运行爬虫主程序
        :return: None
        """
        # 打印目录信息
        print(f"🏠 根目录: {self.ROOT_DIR}")
        print(f"📁 当前工作目录: {os.getcwd()}")

        # 从文件读取IMDB对应的ID列表
        imdb_ids = self.read_imdb_ids_from_file()

        if not imdb_ids:
            print("⚠️ 没有有效的IMDb ID可处理，程序终止")
            print("\n按Enter键退出程序...")
            input()
            return

        # 执行批量处理
        print("🚀 启动IMDb批量处理...")
        success_count, failed_ids, result = self.batch_process(imdb_ids)

        # 结果报告
        print("\n" + "=" * 60)
        print("📊 处理结果:")
        print("=" * 60)
        print(f"📋 处理总数: {len(imdb_ids)}")
        print(f"✅ 成功数量: {success_count}")
        print(f"❌ 失败数量: {len(failed_ids)}")
        print("=" * 60)

        if not result:
            print("⚠️ 处理过程中出现严重错误，结果可能不完整")

        if failed_ids:
            print("\n📝 以下ID处理失败:")
            for fid in failed_ids:
                print(f" - {fid}")

            # 创建失败列表文件
            failed_file = os.path.join(self.ROOT_DIR, self.FAILED_FILE)
            with open(failed_file, "w", encoding="utf-8") as f:
                f.write("\n".join(failed_ids))
            print(f"\n📁 失败ID已保存到: {failed_file}")

        print("\n🎉 程序执行完成，按Enter键退出...")
        input()


if __name__ == "__main__":
    crawler = IMDBCrawler()
    crawler.run()
