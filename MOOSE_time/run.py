# from scrapy import cmdline
# cmdline.execute("scrapy crawl MOOSE_Gitee".split())
from multiprocessing import Process
from scrapy import cmdline
import time
import logging

# 配置参数即可, 爬虫名称，运行频率
confs = [
    {
        "spider_name": "MOOSE",
        "frequency": 86400,
    },
    {
        "spider_name": "MOOSE_Gitee",
        "frequency": 86400,
    },
]


def start_spider(spider_name, frequency):
    args = ["scrapy", "crawl", spider_name,]
    while True:
        start = time.time()
        p = Process(target=cmdline.execute, args=(args,))
        p.start()
        p.join()
        #cmdline.execute("scrapy crawl MOOSE -s LOG_FILE=spider.log")
        logging.debug("### use time: %s" % (time.time() - start))
        time.sleep(frequency)


if __name__ == '__main__':
    for conf in confs:
        process = Process(target=start_spider,
                          args=(conf["spider_name"], conf["frequency"]))
        process.start()
        time.sleep(10)