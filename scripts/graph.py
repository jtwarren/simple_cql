import matplotlib.pyplot as plt


def process_file(filename):
    times = list()
    with open(filename, 'r') as f:
        for line in f:
            [_, time] = line.split(":")
            time = time[:-3]
            times.append(int(time.strip()))
    return times


def plot_times1(times1, times2):
    times1 = [time for time in times1 if time >= 0]
    times2 = [time for time in times2 if time >= 0]
    plt.plot(times1, label="No garbage collection")
    plt.plot(times2, label="Garbage collection")
    plt.xlabel('Discrete time value')
    plt.ylabel('Memory footprint (in MB)')
    plt.axis([0, 1000, 0, 100])
    plt.legend()
    plt.show()


if __name__ == '__main__':
    times1 = process_file('../outputFiles/noGC_memoryFootprint.txt')
    times2 = process_file('../outputFiles/GC_memoryFootprint.txt')
    plot_times1(times1, times2)
