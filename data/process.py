window = 360

with open('xmitdb_orig.txt') as f:
    raw_data = f.read()

data = raw_data.strip().split('\n')
data = [float(x) for x in data]
size = len(data)
target = 4083


def distance(d1, d2):
    d = zip(d1, d2)
    s = sum([(x - y)**2 for (x, y) in d])
    return s**0.5

nn_dist = 99999999.9
nn_i = -1
for i in range(0, size - window):
    if abs(i - target) >= window:
        dist = distance(data[target: target + window],
                        data[i: i + window])
        if dist < nn_dist:
            nn_dist = dist
            nn_i = i

print nn_dist, nn_i
