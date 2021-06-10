import csv
import subprocess

import psycopg2
from app.etlconfig import local
import time

from psycopg2._psycopg import OperationalError
from tqdm import tqdm


class PgLoader:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def print(self):
        print("CONFIG: ")
        print(f"\tin_path: {self.config.in_path}")
        print(f"\tout_path: {self.config.out_path}")
        print(f"\tselected: {self.config.selected}")
        print("\tpg_config:")
        for key in self.config.pg_config:
            print(f"\t \t{key}: {self.config.pg_config[key]}")

    def read_csv_write_to_db(self, name, upsert):
        with open(f"{self.config.in_path}/{name}.csv", newline='') as file:
            csv_file = csv.reader(file, delimiter=',', quotechar='|')
            headers = next(csv_file)
            selected_fields = [f"{head}_{name}" for head in headers]
            first = next(csv_file)
            time_zero = first[0]
            upsert(first, selected_fields)
            for line in csv_file:
                upsert(line, selected_fields)
            return time_zero

    def create_table(self):
        fields = self.get_fields()
        cur = self.conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {self.config.combi_name};")
        typed_fields = [f"{field} numeric " for field in fields]
        cur.execute(f"CREATE TABLE {self.config.combi_name} (time BIGINT, {', '.join(typed_fields)}, "
                    f"CONSTRAINT pk_{self.config.combi_name} PRIMARY KEY (time));")
        self.conn.commit()

    def load_to_db(self):
        cur = self.conn.cursor()
        print(f"loading {len(self.config.selected)} data sets")
        time_zeros = {}
        for name in self.config.selected:
            time_zero = self.read_csv_write_to_db(name, get_upsert(self.config.combi_name, cur))
            time_zeros[name] = time_zero
        self.conn.commit()
        return time_zeros


    def cut_to_first_all_stocks_available(self, time_zeros):
        latest_start = max([time_zeros[key] for key in time_zeros])
        cur = self.conn.cursor()
        cur.execute(f"delete from {self.config.combi_name} where time < {latest_start};")
        self.conn.commit()


    def dump_db(self):
        subprocess.run([
            'psql', "-h", self.config.pg_config["host"], "-U", self.config.pg_config["user"], "-c",
            f"\copy (select * from {self.config.combi_name} order by time) to '{self.config.out_path}' WITH CSV HEADER;"
        ])

    def load_all_data(self):
        print("create table")
        self.create_table()
        print("Write to DB")
        return self.load_to_db()

    def update_initial(self, first_row, fields, cur):
        updated = [first_row[i] if first_row[i] is not None else '0' for i in range(1, len(first_row))]
        updated_k_v = [f"{fields[i]} = '{updated[i]}'" for i in range(0, len(updated))]
        cur.execute(f"update {self.config.combi_name} set {', '.join(updated_k_v)} where time = {first_row[0]};")
        return [first_row[0]] + updated

    def merge_data(self):
        fields = self.get_fields()
        cur = self.conn.cursor()
        cur.execute(f"select time from {self.config.combi_name} order by time ASC;")
        times = cur.fetchall()
        cur.execute(f"select * from {self.config.combi_name} where time = {times[0][0]};")
        first = cur.fetchone()
        previous = self.update_initial(first, fields, cur)
        for time_ in tqdm(times):
            cur.execute(f"select * from {self.config.combi_name} where time = {time_[0]};")
            current = cur.fetchone()
            updated = [current[i] if current[i] is not None else previous[i] for i in range(1, len(current))]
            updated_k_v = [f"{fields[i]} = '{updated[i]}'" for i in range(0, len(updated))]
            cur.execute(f"update {self.config.combi_name} set {', '.join(updated_k_v)} where time = {time_[0]};")
            self.conn.commit()
            previous = [current[0]] + updated


    def get_fields(self):
        return [f"{head}_{name}" for head in self.config.headers for name in self.config.selected]

    def transform_into_one(self):
        pg_is_booting = True
        while pg_is_booting:
            try:
                self.conn = psycopg2.connect(**self.config.pg_config)
                pg_is_booting = False
                print("Start loading of data")
                time_zeros = self.load_all_data()
                print("start at lowest common time")
                self.cut_to_first_all_stocks_available(time_zeros)
                print("merge data ")
                self.merge_data()
                print("dump")
                self.dump_db()
                print("clean up")
                self.conn.close()
                return self.get_fields()
            except OperationalError as e:
                print(e)
                print("Waiting for DB")
                time.sleep(2)


def get_upsert(combi_name, cur):
    def upsert(line, selected_fields):
        field_value_tuples = [f"{selected_fields[i]}='{line[i + 1]}'" for i in range(len(selected_fields))]
        query = f"insert into {combi_name}(time, {', '.join(selected_fields)}) values({','.join(line)}) " \
            f"on conflict on CONSTRAINT pk_{combi_name} do update set {', '.join(field_value_tuples)} " \
            f"where {combi_name}.time = {line[0]};"
        cur.execute(query)
    return upsert


if __name__ == "__main__":
    loader = PgLoader(local())
    loader.transform_into_one()