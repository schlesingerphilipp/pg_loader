import psycopg2
import time
import os
import redis
import functools
from tqdm import tqdm
import json

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
            "db": 3
        }
        self.pg_conn = None
        self.redis_conn = None
        self.init_connections()
        self.pg_table = "all_stocks_no_rep"
        self.include_past_factor = int(os.getenv("past_factor", "20"))
        self.headers = os.getenv("headers", ["open", "close", "high", "low", "volume"])

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


    def prepare_data(self, fields):
        cur = self.pg_conn.cursor()
        cur.execute(f"select count(*) from {self.pg_table}")
        rows = cur.fetchone()[0]
        print(f"There are {rows} rows")
        self.transform_rows(cur, fields, self.include_past_factor)
        print("All done!")


    def transform_rows(self, cur, fields, include_past_factor):
        offset = self.get_non_zero_timestep(cur, fields)
        cur.execute(f"select time from {self.pg_table} where time > {offset} order by time ASC;")
        times = cur.fetchall()
        keys = ['open', 'close', 'high', 'low', 'volume']
        key_set = [key for key in {field.split("_")[0] for field in fields}]
        for key in key_set:
            assert key in keys
        for key in keys:
            assert key in key_set
        first_past_idx = -1 * self.get_past_time_idx(0, include_past_factor) + 1
        per_key = int(len(fields) / len(self.headers))
        for idx in tqdm(range(first_past_idx, len(times))):
            timesteps = self.select_times(times, idx, include_past_factor)
            steps = self.load_past_rows(cur, timesteps, fields)
            if (len(steps) != include_past_factor + 1):
                raise Exception("Missing row for times")
            values = {}
            #Add history of 1 field like btsusd_open
            for i in range(len(fields)):
                column = []
                for step in steps:
                    column.append([float(step[i])])
                values[f"{fields[i]}_hist"] = column
            #Add latest value of one header like open of all stocks
            for i in range(len(self.headers)):
                raw_row = steps[0][i*per_key:(i+1)*per_key]
                row = [float(i) for i in raw_row]
                values[self.headers[i]] = row
            self.redis_conn.set(idx - first_past_idx, json.dumps(values))


    def load_past_rows(self, cur, times, fields):
        query = functools.reduce(lambda a,b: f"{a} OR time = {b[0]}", times, f"select  {', '.join(fields)} from {self.pg_table} where time = {times[0][0]}")
        query += " order by time desc;"
        cur.execute(query)
        return cur.fetchall()

    def select_times(self, times, idx, include_past_factor):
        indicies = [self.get_past_time_idx(idx, i) for i in range(include_past_factor + 1)]
        indicies.reverse()
        return [times[i] for i in indicies]

    def get_past_time_idx(self, idx, i):
        return idx - i * i

    def get_non_zero_timestep(self, cur, fields):
        field__non_zero_constraints = [f"{field}::float > 0" for field in fields]
        field__non_zero_constraints = functools.reduce(lambda x,y: x + " and " + y, field__non_zero_constraints)
        cur.execute(f"select time from {self.pg_table} where {field__non_zero_constraints} order by time ASC limit 1;")
        first_non_zero_time = cur.fetchone()[0]
        return first_non_zero_time


if __name__ == "__main__":
    selected = ["ltcusd", "xrpusd", "btcusd", "eosusd", "ethusd"]
    headers = os.getenv("headers", ["open", "close", "high", "low", "volume"])
    fields = [f"{head}_{name}" for head in headers for name in selected]
    transformer = Transformer()
    transformer.prepare_data(fields)
