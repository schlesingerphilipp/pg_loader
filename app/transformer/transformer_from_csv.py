import time
import redis
from tqdm import tqdm
import json
import csv

class Transformer:
    def __init__(self):

        self.redis_config = {
            "host": 'localhost',
            "port": 6379,
            "db": 1
        }
        self.redis_conn = None
        self.init_connections()

    def init_connections(self):
        redis_is_booting = True
        while redis_is_booting:
            try:
                self.redis_conn = redis.Redis(**self.redis_config)
                redis_is_booting = False
                time.sleep(5)
            except Exception as e:
                print(e)
                print("Waiting for DB")
                time.sleep(2)


    def prepare_data(self):
        self.transform_csv()
        print("All done!")
        self.print_some()


    def transform_csv(self):
        with open("/home/ps/data/all_stocks_no_rep.csv", newline='') as file:
            csv_file = csv.reader(file, delimiter=',', quotechar='|')
            idx = 0
            head = next(csv_file)
            print("HEADER:")
            print(head)
            for line in tqdm(csv_file):
                values = {head[i]: line[i] for i in range(len(line))}
                self.redis_conn.set(idx, json.dumps(values))
                idx += 1



    def print_some(self):
        for i in range(100):
            raw_value = self.redis_conn.get(i+10000).decode("utf-8")
            loaded_value = json.loads(raw_value)
            print(loaded_value)