import csv
import subprocess
import psycopg2
import os


class PgLoader:
    def __init__(self):
        self.in_path: str = os.getenv("in_path", "/data/crypto-currency-pairs-at-minute-resolution")
        self.selected = [x.strip() for x in os.getenv("selected").split(",")]
        self.out_path = os.getenv("out_path", "/data/dump.csv")
        self.headers = os.getenv("headers", ["open", "close", "high", "low", "volume"])
        self.pg_config = {
            "dbname": os.getenv("dbname", "example"),
            "user": os.getenv("user", "example"),
            "host": os.getenv("host", "postgres"),
            "password": os.getenv("password", "example"),
        }

    def print(self):
        print("CONFIG: ")
        print(f"\tin_path: {self.in_path}")
        print(f"\tout_path: {self.out_path}")
        print(f"\tselected: {self.selected}")
        print(f"\theaders: {self.headers}")
        print("\tpg_config:")
        for key in self.pg_config:
            print(f"\t \t{key}: {self.pg_config[key]}")

    def read_csv(self):
        csvs = {}
        for name in self.selected:
            with open(f"{self.in_path}/{name}.csv", newline='') as file:
                csv_file = csv.reader(file, delimiter=',', quotechar='|')
                next(csv_file)  # skip head
                csvs[name] = [line for line in csv_file]
        return csvs

    def create_table(self, conn):
        fields = self.get_fields()
        cur = conn.cursor()
        typed_fields = [f"{field} varchar(255)" for field in fields]
        cur.execute(f"CREATE TABLE IF NOT EXISTS process (time BIGINT, {', '.join(typed_fields)}, "
                    f"CONSTRAINT pk_process PRIMARY KEY (time));")

    def load_to_db(self, csvs, conn):
        cur = conn.cursor()
        for name in csvs:
            csv_file = csvs[name]
            for line in csv_file:
                selected_fields = [f"{name}_{head}" for head in self.headers]
                field_value_tuples = [f"{selected_fields[i - 1]}='{line[i]}'" for i in range(1, len(line))]
                query = f"insert into process(time, {', '.join(selected_fields)}) values({','.join(line)}) " \
                    f"on conflict on CONSTRAINT pk_process do update set {', '.join(field_value_tuples)} " \
                    f"where process.time = {line[0]};"
                cur.execute(query)
        conn.commit()

    def dump_db(self):
        subprocess.run([
            'psql', "-h", self.pg_config["host"], "-U", self.pg_config["user"], "-c",
            f"\copy (select * from process order by time) to '{self.out_path}' WITH CSV HEADER;"
        ])

    def clean_up(self):
        subprocess.check_call([
            'psql', "-h", self.pg_config["host"], "-U", self.pg_config["user"], "-c", f"drop table process;"])

    def load_all_data(self, conn):
        print("Read files")
        csvs = self.read_csv()
        print("create table")
        self.create_table(conn)
        print("Write to DB")
        self.load_to_db(csvs, conn)

    @staticmethod
    def update_initial(first_row, fields, cur):
        updated = [first_row[i] if first_row[i] is not None else '0' for i in range(1, len(first_row))]
        updated_k_v = [f"{fields[i]} = '{updated[i]}'" for i in range(0, len(updated))]
        cur.execute(f"update process set {', '.join(updated_k_v)} where time = {first_row[0]};")
        return [first_row[0]] + updated

    def merge_data(self, conn):
        fields = self.get_fields()
        cur = conn.cursor()
        cur.execute("select * from process order by time ASC limit 1;")
        current = cur.fetchone()
        current = self.update_initial(current, fields, cur)
        next_row = self.get_next(cur, current)
        while next_row:
            updated = [next_row[i] if next_row[i] is not None else current[i] for i in range(1, len(next_row))]
            updated_k_v = [f"{fields[i]} = '{updated[i]}'" for i in range(0, len(updated))]
            cur.execute(f"update process set {', '.join(updated_k_v)} where time = {next_row[0]};")
            conn.commit()
            current = [next_row[0]] + updated
            next_row = self.get_next(cur, current)

    @staticmethod
    def get_next(cursor, current):
        current_time = current[0]
        cursor.execute(f"select * from process where time > {current_time} order by time asc limit 1;")
        return cursor.fetchone()

    def get_fields(self):
        return [f"{name}_{head}" for name in self.selected for head in self.headers]

    def transform_into_one(self):
        pg_is_booting = True
        while pg_is_booting:
            try:
                conn = psycopg2.connect(**self.pg_config)
                pg_is_booting = False
                print("Start loading of data")
                self.load_all_data(conn)
                print("merge data")
                self.merge_data(conn)
                print("Dump DB")
                self.dump_db()
                print("clean up")
                conn.close()
                self.clean_up()
            except Exception as e:
                print(e)
                print("Waiting for DB")


if __name__ == "__main__":
    loader = PgLoader()
    loader.print()
    loader.transform_into_one()
