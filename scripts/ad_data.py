import random
import sys

PROB_IMPRESSION = 0.9
PROB_CLICK = 0.25

IMPRESSION = 0
CLICK = 1

AD_DB = [
    (1000, [100001, 100002, 100003]),
    (2000, [200001, 200002, 200003]),
    (3000, [300001, 300002, 300003]),
    (4000, [400001, 400002, 400003]),
    (5000, [500001, 500002, 500003]),
    (6000, [600001, 600002, 600003]),
    (7000, [700001, 700002, 700003]),
    (8000, [800001, 800002, 800003]),
    (9000, [900001, 900002, 900003])]


def create_ad_data(ips, duration=1800, insert_filename='ad_insert.txt', event_filename='ad_event.txt'):

    insertions = []
    events = []

    insertion_id = 0
    

    # Iterate over each second
    for ts in xrange(duration):
        # print to file each
        for i in xrange(ips):
            ad_item = AD_DB[random.randrange(len(AD_DB))]
            advertiser_id = ad_item[0]
            advertisement_id = random.choice(ad_item[1])
            cost = random.randrange(10, 100)

            insertions.append((insertion_id, advertiser_id, advertisement_id, cost, ts))

            p = random.random()

            if p <= 0.5:
                impression_ts = random.randrange(ts + 1, ts + 30)
            elif p <= 0.80:
                impression_ts = random.randrange(ts + 30, ts + 60)
            elif p <= 0.95:
                impression_ts = random.randrange(ts + 60, ts + 120)
            else:
                impression_ts = random.randrange(ts + 1, ts + 600)

            if random.random() < PROB_IMPRESSION:
                events.append((insertion_id, IMPRESSION, impression_ts))
                if random.random() < PROB_CLICK:
                    events.append((insertion_id, CLICK, impression_ts + 1))

            insertion_id += 1

    events = sorted(events, key=lambda event: event[2])

    # write ad_insert data
    with open(insert_filename, 'w') as f:
        for insertion in insertions:
            insertion_id, advertiser_id, advertisement_id, cost, ts = insertion
            f.write('%s %s %s %s %s\n' % (insertion_id, advertiser_id, advertisement_id, cost, ts))

    # write ad_event data
    with open(event_filename, 'w') as f:
        event_id = 0
        for event in events:
            insertion_id, type, ts = event
            f.write('%s %s %s %s\n' % (event_id, insertion_id, type, ts))
            event_id += 1

if __name__ == '__main__':
    rps = int(sys.argv[1])
    print "qwerty rps", rps
    create_ad_data(rps, 1800, '../ad_insert.txt', '../ad_event.txt')
