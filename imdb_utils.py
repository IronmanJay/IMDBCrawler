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
import glob
import zstandard as zstd
import tarfile
from concurrent.futures import ThreadPoolExecutor
import time
import argparse
import io
from tqdm import tqdm
import shutil
import math



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


class HTMLArchiveCompressor:
    def __init__(self, directory, output_file, compression_level=3, max_workers=None,
                 keep_original=False, verbose=False):
        """
        初始化HTML归档压缩器

        参数:
            directory: 包含HTML文件的目录路径
            output_file: 输出的压缩文件路径
            compression_level: Zstd压缩级别(1-22, 默认3)
            max_workers: 线程池大小(默认=CPU核心数×2)
            keep_original: 是否保留原始HTML文件(默认False)
            verbose: 是否显示详细输出(默认False)
        """
        self.directory = directory
        self.output_file = output_file
        self.compression_level = compression_level
        self.keep_original = keep_original
        self.verbose = verbose

        # 自动确定合适的线程数
        self.max_workers = max_workers or (os.cpu_count() * 2 if os.cpu_count() else 8)

        # 收集HTML文件
        self.html_files = self._find_html_files()
        self.total_files = len(self.html_files)
        self.processed_files = 0
        self.start_time = None

    def _find_html_files(self):
        """查找目录中的所有HTML文件"""
        pattern = os.path.join(self.directory, '**/*.html')
        return glob.glob(pattern, recursive=True)

    def compress(self):
        """将所有HTML文件压缩到单个压缩包"""
        if not self.html_files:
            print("No HTML files found in the directory!")
            return False

        self.start_time = time.time()
        print(f"Found {self.total_files} HTML files.")
        print(f"Starting compression to {self.output_file}...")
        print(f"Compression level: {self.compression_level}")
        print(f"Original files will {'be kept' if self.keep_original else 'be deleted'}")

        try:
            # 创建输出目录（如果需要）
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

            # 使用Zstd压缩器
            cctx = zstd.ZstdCompressor(level=self.compression_level, threads=0)

            # 创建进度条
            progress_bar = tqdm(total=self.total_files, disable=not self.verbose,
                                desc="Compressing files", unit="file")

            # 打开输出文件
            with open(self.output_file, 'wb') as f_out:
                # 创建Zstd压缩流
                with cctx.stream_writer(f_out) as compressor:
                    # 创建tar归档器
                    with tarfile.open(fileobj=compressor, mode='w|') as tar:
                        # 添加所有HTML文件到tar归档
                        for file_path in self.html_files:
                            # 计算在归档中的相对路径
                            arcname = os.path.relpath(file_path, self.directory)

                            # 添加文件到tar
                            tar.add(file_path, arcname=arcname)

                            # 更新进度
                            self.processed_files += 1
                            progress_bar.update(1)

                            # 删除原始文件（如果设置）
                            if not self.keep_original:
                                os.remove(file_path)

            progress_bar.close()

            # 显示最终结果
            elapsed_time = time.time() - self.start_time
            print(f"\nCompression completed!")
            print(f"Successfully compressed {self.total_files} files into {self.output_file}")
            print(f"Total time: {elapsed_time:.2f} seconds")
            print(f"Average speed: {self.total_files / elapsed_time:.2f} files/sec")

            return True
        except Exception as e:
            print(f"Error during compression: {str(e)}")
            return False


class ZstdTarExtractor:
    def __init__(self, archive_path, extract_dir, verbose=False):
        """
        初始化Zstd Tar解压器

        参数:
            archive_path: .tar.zst压缩文件路径
            extract_dir: 解压目标目录
            verbose: 是否显示详细输出
        """
        self.archive_path = archive_path
        self.extract_dir = extract_dir
        self.verbose = verbose

        # 验证文件存在
        if not os.path.exists(archive_path):
            raise FileNotFoundError(f"Archive file not found: {archive_path}")

    def extract(self):
        """解压.tar.zst文件"""
        # 创建解压目录
        os.makedirs(self.extract_dir, exist_ok=True)

        print(f"Extracting {os.path.basename(self.archive_path)} to {self.extract_dir}")

        # 获取压缩文件大小用于进度条
        total_size = os.path.getsize(self.archive_path)

        try:
            # 使用Zstd解压器
            dctx = zstd.ZstdDecompressor()

            # 打开压缩文件
            with open(self.archive_path, 'rb') as f_in:
                # 创建解压流
                with dctx.stream_reader(f_in) as decompressed:
                    # 使用tarfile打开解压后的数据流
                    with tarfile.open(fileobj=decompressed, mode='r|') as tar:
                        # 创建进度条
                        pbar = tqdm(desc="Extracting", total=total_size,
                                    unit='B', unit_scale=True, disable=not self.verbose)

                        # 提取所有文件
                        for member in tar:
                            # 显示当前提取的文件（如果verbose）
                            if self.verbose:
                                print(f"Extracting: {member.name}")

                            tar.extract(member, self.extract_dir)

                            # 更新进度条（基于已处理的数据量）
                            pbar.update(f_in.tell() - pbar.n)

                        pbar.close()

            print(f"Successfully extracted to {self.extract_dir}")
            return True

        except Exception as e:
            print(f"Extraction failed: {str(e)}")
            return False


class FastHTMLBatchSplitter:
    def __init__(self, source_dir, batch_size=20000, target_dir=None, workers=8):
        """
        超高速HTML文件分批器

        参数:
            source_dir: 源目录路径
            batch_size: 每批文件数 (默认20,000)
            target_dir: 目标目录 (默认: source_dir/batches)
            workers: 并行线程数 (默认8)
        """
        self.source_dir = os.path.normpath(source_dir)
        self.batch_size = batch_size
        self.target_root = target_dir or os.path.join(source_dir, "batches")
        self.workers = workers
        self.file_chunks = []
        self.processed_files = 0  # 新增：跟踪已处理文件数

    def _find_html_files(self):
        """多线程快速扫描HTML文件"""
        print("🚀 正在加速扫描HTML文件...")
        all_files = []

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            # 并行遍历目录
            for root, _, files in os.walk(self.source_dir):
                if os.path.normpath(root).startswith(os.path.normpath(self.target_root)):
                    continue
                all_files.extend(os.path.join(root, f) for f in files if f.lower().endswith('.html'))

        return all_files

    def _prepare_batches(self):
        """预分配文件到批次"""
        total_files = len(self.html_files)
        num_batches = math.ceil(total_files / self.batch_size)

        # 预切割文件列表避免动态分配
        self.file_chunks = [
            self.html_files[i * self.batch_size: (i + 1) * self.batch_size]
            for i in range(num_batches)
        ]

    def _process_batch(self, batch_num, files, pbar):
        """处理单个批次 (线程安全)"""
        batch_dir = os.path.join(self.target_root, f"batch_{batch_num + 1:03d}")

        for src_file in files:
            try:
                rel_path = os.path.relpath(src_file, self.source_dir)
                dst_file = os.path.join(batch_dir, rel_path)
                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                shutil.move(src_file, dst_file)
                if pbar:  # 新增：安全更新进度条
                    pbar.update(1)
            except Exception as e:
                print(f"\n⚠️ 处理文件失败: {src_file} - {str(e)}")

    def split_into_batches(self):
        """多线程分批处理"""
        self.html_files = self._find_html_files()

        if not self.html_files:
            print("❌ 未找到HTML文件！")
            return False

        print(f"📊 共找到 {len(self.html_files):,} 个HTML文件")
        self._prepare_batches()

        # 创建目标目录
        os.makedirs(self.target_root, exist_ok=True)

        # 初始化进度条
        with tqdm(total=len(self.html_files), desc="⚡ 加速处理", unit="文件") as pbar:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = []
                for i, chunk in enumerate(self.file_chunks):
                    future = executor.submit(self._process_batch, i, chunk, pbar)
                    futures.append(future)

                # 等待所有任务完成
                for future in futures:
                    future.result()

        # 安全获取处理速度
        speed = len(self.html_files) / (pbar.format_dict['elapsed'] or 1)
        print(f"\n✅ 处理完成！平均速度: {speed:.1f} 文件/秒")
        return True


if __name__ == "__main__":
    state = "splithtml"

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
    elif state == "compressor":
        # 设置命令行参数解析
        parser = argparse.ArgumentParser(description='HTML文件归档压缩工具')
        parser.add_argument('directory', type=str, help='包含HTML文件的目录路径')
        parser.add_argument('output', type=str, help='输出压缩文件路径')
        parser.add_argument('--level', type=int, default=3,
                            help='压缩级别(1-22, 默认3)')
        parser.add_argument('--workers', type=int, default=None,
                            help='线程数(默认=自动检测)')
        parser.add_argument('--keep', action='store_true',
                            help='保留原始文件(默认删除)')
        parser.add_argument('--verbose', action='store_true',
                            help='显示详细输出')

        args = parser.parse_args()

        # 创建压缩器实例并执行压缩
        compressor = HTMLArchiveCompressor(
            directory=args.directory,
            output_file=args.output,
            compression_level=args.level,
            max_workers=args.workers,
            keep_original=args.keep,
            verbose=args.verbose
        )

        success = compressor.compress()

        if success:
            print("All files compressed successfully into a single archive!")
        else:
            print("Compression failed.")
    if state == "decompressor":
        # 设置命令行参数解析
        parser = argparse.ArgumentParser(description='Zstd Tar解压工具')
        parser.add_argument('archive', type=str, help='.tar.zst文件路径')
        parser.add_argument('output_dir', type=str, help='解压目标目录')
        parser.add_argument('--verbose', action='store_true', help='显示详细输出')

        args = parser.parse_args()

        # 创建解压器实例并执行解压
        extractor = ZstdTarExtractor(
            archive_path=args.archive,
            extract_dir=args.output_dir,
            verbose=args.verbose
        )

        success = extractor.extract()

        if success:
            print("解压成功！")
        else:
            print("解压失败，请检查错误信息。")
    elif state=="splithtml":
        parser = argparse.ArgumentParser(description='超高速HTML文件分批器')
        parser.add_argument('source_dir', help='源目录路径')
        parser.add_argument('--batch-size', type=int, default=20000, help='每批文件数')
        parser.add_argument('--target-dir', help='自定义目标目录')
        parser.add_argument('--workers', type=int, default=8, help='并行线程数')

        args = parser.parse_args()

        print(f"⚡ 启动超高速处理 (线程数: {args.workers})")
        splitter = FastHTMLBatchSplitter(
            source_dir=args.source_dir,
            batch_size=args.batch_size,
            target_dir=args.target_dir,
            workers=args.workers
        )

        splitter.split_into_batches()
