import csv
from app.pre_processor.perfect_trend import preprocess_with_trends, PreprocessorConf
def write_test_csv(out_path, name):
    headers = ["open", "close", "high", "low", "volume"]
    rows = [[0,i,i,i,i,i] for i in range(1,10000)]
    with open(f"{out_path}{name}.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(headers)
        writer.writerows(rows)

def read_csv(name, in_path):
    with open(f"{in_path}/{name}.csv", newline='') as file:
        csv_file = csv.reader(file, delimiter=',', quotechar='|')
        next(csv_file)  # skip head
        return [[float(x)for x in line] for line in csv_file]


config = PreprocessorConf()
selected = "test"
write_test_csv(config.in_path, selected)
preprocess_with_trends([selected], config.headers, config.in_path, config.out_path)
expected = [[1.0,1.5,5.5,30.5,60.5,720.5] for i in range(5)]
expected = [x for line in expected for x in line] #flatten
expected.insert(0, 0.0) # time dummy
test_with_trends = read_csv(selected, config.out_path)
actual = test_with_trends[0]
assert actual == expected
