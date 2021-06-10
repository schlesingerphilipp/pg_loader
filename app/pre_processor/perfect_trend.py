import csv
from tqdm import tqdm



class PreprocessorConf:
    def __init__(self):
        self.in_path: str = "/home/ps/data/crypto-currency-pairs-at-minute-resolution/no-gaps/"
        self.selected = ["ltcusd","xrpusd","btcusd","eosusd","ethusd"]
        self.out_path = "/home/ps/data/crypto-currency-pairs-at-minute-resolution/with-trend/"
        self.headers = ["open", "open_t1", "open_t10", "open_t60", "open_t120", "open_t1440",
                        "close", "close_t1", "close_t10", "close_t60", "close_t120", "close_t1440",
                        "high","high_t1", "high_t10", "high_t60", "high_t120", "high_t1440",
                        "low", "low_t1", "low_t10", "low_t60", "low_t120", "low_t1440",
                        "volume", "volume_t1", "volume_t10", "volume_t60", "volume_t120", "volume_t1440"]


    def print(self):
        print("CONFIG: ")
        print(f"\tin_path: {self.in_path}")
        print(f"\tout_path: {self.out_path}")
        print(f"\tselected: {self.selected}")
        print(f"\theaders: {self.headers}")




def read_csv(name, in_path):
    with open(f"{in_path}{name}.csv", newline='') as file:
        csv_file = csv.reader(file, delimiter=',', quotechar='|')
        head = next(csv_file)  # skip head
        return head, [[float(x)for x in line] for line in csv_file]

def write_csv(with_trends, name, out_path, headers):
    with open(f"{out_path}{name}.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(headers)
        writer.writerows(with_trends)




def add_trends(headers, csv_file):
    trend_windows = [2,10,60,120,1440]
    window = csv_file[:1441]
    with_trend = []
    for line in tqdm(csv_file[:len(csv_file) - 1440]):
        trend = get_trend(headers, window, trend_windows)
        with_trend.append(trend)
        window.append(line)
        window = window[1:]
    return with_trend

def get_trend(headers, currents, trend_windows):
    trends = [currents[0][0]] # time

    for i in range(1,len(headers)):
        trends.append(currents[0][i]) # original value
        values = [current[i] for current in currents]
        for t in trend_windows:
            trends.append((sum(values[:t]) / t) - values[0])
    return trends


def preprocess_with_trends(selected, trend_headers, in_path, out_path):
    for selected in selected:
        headers, csv_file = read_csv(selected, in_path)
        with_trends = add_trends(headers, csv_file)
        assert len(with_trends[0]) == 1 + len(trend_headers)
        write_csv(with_trends, selected, out_path, trend_headers)


if __name__ == "__main__":
    config = PreprocessorConf()
    preprocess_with_trends(config.selected, config.headers, config.in_path, config.out_path)
