def compute_answer_time_window(filename):
    data = dict()
    with open(filename, 'r') as f:
        for line in f:
            line_eles = line.split(" ")
            ts = int(line_eles[-1])
            if ts not in data:
                data[ts] = list()
            data[ts].append(int(line_eles[1]))
    for ts in data:
        valid_data = [ele for ele in data[ts] if ele >= 20]
        avg = sum(valid_data) / len(valid_data)
        print avg, ts


def compute_answer_tuple_window(filename):
    data = list()
    boundaries = dict()
    cnt = 0
    max_ts = 0
    with open(filename, 'r') as f:
        for line in f:
            line_eles = line.split(" ")
            ts = int(line_eles[-1])
            max_ts = ts
            data.append(int(line_eles[1]))
            if (ts - 1) not in boundaries and ts >= 1:
                boundaries[ts-1] = cnt - 1
            cnt += 1
    boundaries[max_ts] = cnt - 1
    for ts in boundaries:
        start = 0
        if (boundaries[ts] - 29 >= 0):
            start = boundaries[ts] - 29
        valid_data = [ele for ele in data[start: boundaries[ts]+1] if ele >= 20]
        avg = sum(valid_data) / len(valid_data)
        print avg, ts


if __name__ == '__main__':
    compute_answer_tuple_window('/Users/Deepak/Documents/6.830/lab/aggregation_test.txt')
