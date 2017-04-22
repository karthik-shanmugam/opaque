import sys
import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from itertools import groupby

filename = sys.argv[1]
data = json.load(open(filename, "r"))

opaque_lc = [(item["groups"], item["size"], item["time"]) for item in data if item["low_cardinality"] == True and item["system"] == "opaque"]
opaque_hc = [(item["groups"], item["size"], item["time"]) for item in data if item["low_cardinality"] == False and item["system"] == "opaque"]


def avg(l):
    key = lambda x: x[0]

    output = []
    for k, g in groupby(sorted(l, key=key), key=key):
        vals = list(g)
        mean = reduce(lambda x, y: (x[0], x[1], x[2] + y[2]), vals)
        output.append((mean[0], mean[1], mean[2] / len(vals)))
    return output

opaque_lc = avg(opaque_lc)
opaque_hc = avg(opaque_hc)

def groups(l):
    return [item[0] for item in l]
def size(l):
    return [item[1] for item in l]
def runtime(l):
    return [item[2] for item in l]



plt.plot(groups(opaque_lc), runtime(opaque_lc), "r", label="low cardinality algorithm"),
plt.plot(groups(opaque_hc), runtime(opaque_hc), "b", label="high cardinality algorithm")
plt.ylabel('time (ms)')
plt.xlabel('distinct groups')
plt.title('Opaque Aggregation %d Rows: Distinct Groups vs Time' % size(opaque_lc)[0])
plt.legend(loc="upper left")

print avg(opaque_lc)

plt.show()

