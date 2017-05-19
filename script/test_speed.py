a = 1
b = [x + 1 for x in range(10000)]

def re_a():
    return a

def re_b():
    return b

import time

print time.time()
for x in range(50000000):
    re_a()

print time.time()
for x in range(50000000):
    re_b()

print time.time()