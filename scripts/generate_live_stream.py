import time
import random


def generate_stream():
    possible_msgs = [
        "Hadoop log message",
        "Extraneous log message",
        "Pig log message",
        "Hive log message",
        "HDFS error"
    ]
    with open("output.txt", 'w', 0) as f:
        while True:
            multiplier = 0.001
            if random.random() > 0.9:
                multiplier = 0.01
            time.sleep(multiplier * random.random())
            f.write(possible_msgs[int(random.random() * 5)] + "\n")


if __name__ == '__main__':
    generate_stream()
