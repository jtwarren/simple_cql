import random
import sys

HASHTAGS = ['#mit', '#mit', '#mit', '#mit', '#brown', '#colubia', '#cornell', '#dartmouth', '#harvard', '#princeton', '#penn', '#yale']
LOCATIONS = ['Providence', 'NewYork', 'Ithaca', 'Hanover', 'Cambridge', 'Cambridge', 'Princeton', 'Philadelphia', 'NewHaven']

def create_ad_data(ips, duration=1800, filename='trending.txt'):

    tweets = []
    
    # Iterate over each second
    for ts in xrange(duration):
        count = 0
        # print to file each
        for i in xrange(ips):
            hashtag = random.choice(HASHTAGS)
            location = random.choice(LOCATIONS)
            tweets.append((hashtag, location, ts))

    # write ad_insert data
    with open(filename, 'w') as f:
        for tweet in tweets:
            f.write('%s %s %s\n' % tweet)

if __name__ == '__main__':
    rps = int(sys.argv[1])
    print "qwerty rps", rps
    create_ad_data(rps, 1800, '../trending.txt')
