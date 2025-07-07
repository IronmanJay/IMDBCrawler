# 一、项目介绍
本项目用于爬取IMDB网站的HTML页面，根据电影ID进行爬取，所用的爬取方法包括：

1. playwright
2. requests
3. selenium

使用任意一种方法爬取均可。

# 二、使用方法
1. imdb_crawler_playwright_multi_threaded.py: 直接运行
2. imdb_crawler_playwright_single_threaded.py: 直接运行
3. imdb_crawler_request.py: 首先获取IMDB网站的登录Cookie，然后替换目标Cookie，再运行
4. imdb_crawler_selenium.py: 直接运行
5. imdb_utils.py: 一些辅助工具，可根据自身需求使用