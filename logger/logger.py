import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(filename)s: %(message)s',
    filename='/home/navarro/hadoop_spark/logs/auctions/bronze/auctions.log',
    encoding='utf-8',
    filemode='a',
    datefmt='%Y-%m-%d %H:%M:%S'
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(filename)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S"))

logging.getLogger().addHandler(console_handler)
