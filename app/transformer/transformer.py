import psycopg2
import time
import os
import redis
import functools
from tqdm import tqdm
import json
import sys

class Transformer:
    def __init__(self):
        self.pg_config = {
            "dbname": os.getenv("dbname", "example"),
            "user": os.getenv("user", "example"),
            "host": os.getenv("host", "localhost"),
            "password": os.getenv("password", "example"),
        }
        self.redis_config = {
            "host": 'localhost',
            "port": 6379,
            "db": 1
        }
        self.pg_conn = None
        self.redis_conn = None
        self.init_connections()
        self.pg_table = "all_stocks_no_rep"

    def init_connections(self):
        pg_is_booting = True
        redis_is_booting = True
        while pg_is_booting or redis_is_booting:
            try:
                self.pg_conn = psycopg2.connect(**self.pg_config)
                pg_is_booting = False
                self.redis_conn = redis.Redis(**self.redis_config)
                redis_is_booting = False
                time.sleep(20)
            except Exception as e:
                print(e)
                print("Waiting for DB")
                time.sleep(2)


    def prepare_data(self, fields):
        cur = self.pg_conn.cursor()
        cur.execute(f"select count(*) from {self.pg_table}")
        rows = cur.fetchone()[0]
        print(f"There are {rows} rows")
        self.transform_rows(cur, fields)
        print("All done!")


    def transform_rows(self, cur, fields):
        cur.execute(f"select time from {self.pg_table} order by time ASC;")
        times = cur.fetchall()
        idx = 0
        for time in tqdm(times):
            step = self.load_row(cur, time[0], fields)
            values = {fields[i]: step[i] for i in range(len(step))}
            self.redis_conn.set(idx, json.dumps(values))
            idx += 1


    def load_row(self, cur, time, fields):
        query = f"select {', '.join(fields)} from {self.pg_table} where time = {time}"
        cur.execute(query)
        return cur.fetchone()

    def select_times(self, times, idx, include_past_factor):
        indicies = [self.get_past_time_idx(idx, i) for i in range(include_past_factor + 1)]
        indicies.reverse()
        return [times[i] for i in indicies]

    def get_past_time_idx(self, idx, i):
        return idx - i * i


