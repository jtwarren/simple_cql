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
            multiplier = 1.0
            if random.random() > 0.9:
                multiplier = 5.0
            time.sleep(multiplier * random.random())
            f.write(possible_msgs[int(random.random() * 4.2)] + "\n")


if __name__ == '__main__':
    generate_stream()
