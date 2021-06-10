import csv
from tqdm import tqdm



class PreprocessorConf:
    def __init__(self):
        self.in_path: str = "/home/ps/data/crypto-currency-pairs-at-minute-resolution/minutes-since/"
        self.selected = ["ltcusd","xrpusd","btcusd","eosusd","ethusd"]
        self.out_path = "/home/ps/data/crypto-currency-pairs-at-minute-resolution/no-gaps/"


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

def write_csv(with_trends, name, out_path, headers):
    with open(f"{out_path}{name}.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(headers)
        writer.writerows(with_trends)




def fill_gaps(csv_file):
    filled = []
    for i in range(len(csv_file)-1):
        current = csv_file[i]
        next_ = csv_file[i+1]
        t1 = current[0]
        t2 = next_[0]
        td = int(t2 - t1)
        for j in range(td+1):
            filler = current
            filler[0] = filler[0] + j
            filled.append(filler)
    return filled


def main(selected, in_path, out_path):
    for name in selected:
        headers, csv_file = read_csv(name, in_path)
        filled = fill_gaps(csv_file)
        write_csv(filled, name, out_path, headers)



if __name__ == "__main__":
    config = PreprocessorConf()
    main(config.selected, config.in_path, config.out_path)
