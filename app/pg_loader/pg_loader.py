import csv
import subprocess

import psycopg2
import os
import time

from psycopg2._psycopg import OperationalError
from tqdm import tqdm


class PgLoader:
    def __init__(self):
        self.selected = ["ltcusd", "xrpusd", "btcusd", "eosusd", "ethusd"]
        self.headers = ["open", "open_t1", "open_t10", "open_t60", "open_t120", "open_t1440",
                        "close", "close_t1", "close_t10", "close_t60", "close_t120", "close_t1440",
                        "high", "high_t1", "high_t10", "high_t60", "high_t120", "high_t1440",
                        "low", "low_t1", "low_t10", "low_t60", "low_t120", "low_t1440",
                        "volume", "volume_t1", "volume_t10", "volume_t60", "volume_t120", "volume_t1440"]
        self.in_path: str =  '/home/ps/data/crypto-currency-pairs-at-minute-resolution/with-trend' #os.getenv("in_path", "/data/with-trend") #
        self.selected = ["ltcusd","xrpusd","btcusd","eosusd","ethusd"]
        self.combi_name = "all_stocks_no_rep"  # '_'.join(selected[1:len(selected)])
        self.out_path = os.getenv("out_path", "/home/ps/data/") + f"{self.combi_name}.csv"
        self.pg_config = {
            "dbname": os.getenv("dbname", "example"),
            "user": os.getenv("user", "example"),
            "host": os.getenv("host", "localhost"),
            "password": os.getenv("password", "example"),
        }
        self.conn = None

    def print(self):
        print("CONFIG: ")
        print(f"\tin_path: {self.in_path}")
        print(f"\tout_path: {self.out_path}")
        print(f"\tselected: {self.selected}")
        print("\tpg_config:")
        for key in self.pg_config:
            print(f"\t \t{key}: {self.pg_config[key]}")

    def read_csv(self, name):
        with open(f"{self.in_path}/{name}.csv", newline='') as file:
            csv_file = csv.reader(file, delimiter=',', quotechar='|')
            head = next(csv_file)
            return (head, [line for line in csv_file])

    def create_table(self):
        fields = self.get_fields()
        cur = self.conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {self.combi_name};")
        typed_fields = [f"{field} numeric" for field in fields]
        cur.execute(f"CREATE TABLE {self.combi_name} (time BIGINT, {', '.join(typed_fields)}, "
                    f"CONSTRAINT pk_{self.combi_name} PRIMARY KEY (time));")
        self.conn.commit()

    def load_to_db(self):
        cur = self.conn.cursor()
        print(f"loading {len(self.selected)} data sets")
        for name in self.selected:
            headers, csv_file = self.read_csv(name)
            print(f"loading {name} with {len(csv_file)} lines")
            print("HEADERS:")
            print(headers)
            for line in tqdm(csv_file):
                selected_fields = [f"{head}_{name}" for head in headers]
                field_value_tuples = [f"{selected_fields[i]}='{line[i + 1]}'" for i in range(len(selected_fields))]
                query = f"insert into {self.combi_name}(time, {', '.join(selected_fields)}) values({','.join(line)}) " \
                    f"on conflict on CONSTRAINT pk_{self.combi_name} do update set {', '.join(field_value_tuples)} " \
                    f"where {self.combi_name}.time = {line[0]};"
                cur.execute(query)
        self.conn.commit()

    def dump_db(self):
        subprocess.run([
            'psql', "-h", self.pg_config["host"], "-U", self.pg_config["user"], "-c",
            f"\copy (select * from {self.combi_name} order by time) to '{self.out_path}' WITH CSV HEADER;"
        ])

    def clean_up(self):
        subprocess.check_call([
            'psql', "-h", self.pg_config["host"], "-U", self.pg_config["user"], "-c", f"drop table {self.combi_name};"])

    def load_all_data(self):
        print("create table")
        self.create_table()
        print("Write to DB")
        self.load_to_db()

    def update_initial(self, first_row, fields, cur):
        updated = [first_row[i] if first_row[i] is not None else '0' for i in range(1, len(first_row))]
        updated_k_v = [f"{fields[i]} = '{updated[i]}'" for i in range(0, len(updated))]
        cur.execute(f"update {self.combi_name} set {', '.join(updated_k_v)} where time = {first_row[0]};")
        return [first_row[0]] + updated

    def merge_data(self):
        fields = self.get_fields()
        cur = self.conn.cursor()
        cur.execute(f"select time from {self.combi_name} order by time ASC;")
        times = cur.fetchall()
        cur.execute(f"select * from {self.combi_name} where time = {times[0][0]};")
        first = cur.fetchone()
        previous = self.update_initial(first, fields, cur)
        for time_ in tqdm(times):
            cur.execute(f"select * from {self.combi_name} where time = {time_[0]};")
            current = cur.fetchone()
            updated = [current[i] if current[i] is not None else previous[i] for i in range(1, len(current))]
            updated_k_v = [f"{fields[i]} = '{updated[i]}'" for i in range(0, len(updated))]
            cur.execute(f"update {self.combi_name} set {', '.join(updated_k_v)} where time = {time_[0]};")
            self.conn.commit()
            previous = [current[0]] + updated


    def get_fields(self):
        return [f"{head}_{name}" for head in self.headers for name in self.selected]

    def transform_into_one(self):
        pg_is_booting = True
        while pg_is_booting:
            try:
                self.conn = psycopg2.connect(**self.pg_config)
                pg_is_booting = False
                print("Start loading of data")
                self.load_all_data()
                print("merge data")
                #self.merge_data()
                print("dump")
                #self.dump_db()
                print("clean up")
                self.conn.close()
                return self.get_fields()
            except OperationalError as e:
                print(e)
                print("Waiting for DB")
                time.sleep(2)
