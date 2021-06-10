import csv
from tqdm import tqdm
import time
import calendar

class PreprocessorConf:
    def __init__(self):
        self.in_path: str = "/home/ps/data/crypto-currency-pairs-at-minute-resolution/"
        self.selected = ["ltcusd","xrpusd","btcusd","eosusd","ethusd"]
        self.out_path = "/home/ps/data/crypto-currency-pairs-at-minute-resolution/minutes-since/"


    def print(self):
        print("CONFIG: ")
        print(f"\tin_path: {self.in_path}")
        print(f"\tout_path: {self.out_path}")
        print(f"\tselected: {self.selected}")




def read_csv(name, in_path):
    with open(f"{in_path}{name}.csv", newline='') as file:
        csv_file = csv.reader(file, delimiter=',', quotechar='|')
        head = next(csv_file)  # skip head
        return head, [[float(x)for x in line] for line in csv_file]

def write_csv(csv_file, name, out_path, headers):
    with open(f"{out_path}{name}.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(headers)
        writer.writerows(csv_file)




def minute_since_2013(csv_file, t0):
    for line in csv_file:
        line[0] = int(((line[0] / 60000) - t0))
    return csv_file



def main(selected, in_path, out_path):
    t0 = calendar.timegm(time.strptime("1 Jan 13", "%d %b %y")) / 60
    for name in selected:
        headers, csv_file = read_csv(name, in_path)
        csv_file_minutes_since_2013 = minute_since_2013(csv_file, t0)
        csv_file_minutes_since_2013.sort(key=lambda x: x[0])
        write_csv(csv_file_minutes_since_2013, name, out_path, headers)



if __name__ == "__main__":
    config = PreprocessorConf()
    main(config.selected, config.in_path, config.out_path)
