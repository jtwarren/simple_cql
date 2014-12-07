import random

PROB_ERROR = 0.5

ERRORS = ['server_500', 'server_404', 'server_401', 'server_418']


def create_ad_data(ips, duration=1800, filename='error_log.txt'):

    log_messages = []
    
    # Iterate over each second
    for ts in xrange(duration):
        count = 0
        # print to file each
        for i in xrange(ips):
            if random.random() < PROB_ERROR:
                msg = random.choice(ERRORS)
                log_messages.append((msg, ts))
                if msg == 'hadoop':
                    count += 1
            else:
                log_messages.append(('pass', ts))

        # print count

    # write ad_insert data
    with open(filename, 'w') as f:
        for message in log_messages:
            f.write('%s %s\n' % message)

if __name__ == '__main__':
    create_ad_data(25000, 30, '../error_log.txt')
