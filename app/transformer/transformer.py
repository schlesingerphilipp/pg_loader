import psycopg2
import time
import os
import redis
import functools
from tqdm import tqdm

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
        self.pg_table = "all_stocks"

    def init_connections(self):
        pg_is_booting = True
        redis_is_booting = True
        while pg_is_booting or redis_is_booting:
            try:
                self.pg_conn = psycopg2.connect(**self.pg_config)
                pg_is_booting = False
                self.redis_conn = redis.Redis(**self.redis_config)
                redis_is_booting = False
            except Exception as e:
                print(e)
                print("Waiting for DB")
                time.sleep(2)


    def prepare_data(self, include_past_factor):
        cur = self.pg_conn.cursor()
        cur.execute(f"select count(*) from {self.pg_table}")
        rows = cur.fetchone()[0]
        print(f"There are {rows} rows")
        self.transform_rows(cur, include_past_factor)
        print("All done!")


    def transform_rows(self, cur, include_past_factor):
        cur.execute(f"select time from {self.pg_table} order by time ASC;")
        times = cur.fetchall()
        first_past_idx = -1 * self.get_past_time_idx(0, include_past_factor) + 1
        for idx in tqdm(range(first_past_idx, len(times))):
            timesteps = self.select_times(times, idx, include_past_factor)
            steps = self.load_past_rows(cur, timesteps)
            if (len(steps) != include_past_factor + 1):
                raise Exception("Missing row for times")
            row = []
            zeros = [0 for _ in range(len(steps))]
            for i in range(1,len(steps[0])):
                column = []
                for step in steps:
                    column.append(step[i])
                row.append(column)
                row.append(zeros)
            row = [key for sublist in row for key in sublist]
            row = functools.reduce(lambda a,b: f"{a},{b}", row)
            self.redis_conn.set(idx - first_past_idx, row)


    def load_past_rows(self, cur, times):
        query = functools.reduce(lambda a,b: f"{a} OR time = {b[0]}", times, f"select * from {self.pg_table} where time = {times[0][0]}")
        query += " order by time asc;"
        cur.execute(query)
        return cur.fetchall()

    def select_times(self, times, idx, include_past_factor):
        indicies = [self.get_past_time_idx(idx, i) for i in range(include_past_factor + 1)]
        indicies.reverse()
        return [times[i] for i in indicies]

    def get_past_time_idx(self, idx, i):
        return idx - i * i


