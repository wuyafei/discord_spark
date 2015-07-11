import timeit
import numpy as np
window = 360

with open('xmitdb_orig.txt') as f:
    raw_data = f.read()

data = raw_data.strip().split('\n')
data = np.array([float(x) for x in data])
size = data.shape[0]
r = 1.55
distance_cnt = 0

def distance(d1, d2):
    global distance_cnt
    distance_cnt += 1
    return np.linalg.norm(d1 - d2)

start = timeit.default_timer()
discords = []
early_abandon_idxs = []
for i in range(0, size - window + 1):
    if i in early_abandon_idxs:
        continue
    nn_dist = 99999999.9
    nn_idx = -1
    early_abandon = False
    for j in range(0, size - window + 1):
        if abs(i - j) >= window:
            dist = distance(data[j: j + window],
                            data[i: i + window])
            if dist < r:
                early_abandon_idxs.append(j)
                early_abandon = True
                break
            if dist < nn_dist:
                nn_dist = dist
                nn_idx = j
    if early_abandon:
        continue
    if len(discords) == 0 or i - discords[-1][0] >= window:
        discords.append((i, nn_dist))
    elif nn_dist > discords[-1][1]:
        discords.pop()
        discords.append((i, nn_dist))
print "time cost: ", timeit.default_timer() - start
for x in discords:
    print x[0], x[1]
print 'distance_cnt=', distance_cnt
