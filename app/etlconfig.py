

selected = ["btcusd", "ltcusd", "xrpusd", "eosusd", "ethusd"]
combi_name = "all_stocks_no_rep"
pg_config = {
            "dbname": "example",
            "user": "example",
            "host": "localhost",
            "password": "example",
        }
redis_config = {
            "host": 'localhost',
            "port": 6379,
        }
trends = [1,10,60,120,1440]
ochlv = ["open", "close", "high", "low", "volume"]


class Config:
    def __init__(self, in_path, out_path, redis_db):
        self.selected = selected
        self.in_path = in_path
        self.combi_name = '_'.join(selected)
        self.out_path = out_path + f"{self.combi_name}.csv"
        self.redis_config = redis_config
        self.redis_config["db"] = redis_db
        self.trends = trends
        self.headers = ochlv + [f"{key}_t{t}" for t in trends for key in ochlv]
        self.pg_config = pg_config


redis_db = 1


def local():
    return Config("/home/ps/data/crypto-currency-pairs-at-minute-resolution/with-trend",
                  "/home/ps/data/crypto-currency-pairs-at-minute-resolution/", redis_db)


def docker():
    return Config("/data/with-trend", "/data/", redis_db)
