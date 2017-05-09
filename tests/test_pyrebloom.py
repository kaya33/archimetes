import pyreBloom

p = pyreBloom.pyreBloom('123', 500, 0.01)
p.extend('w')
print p.bits
print p.hashes
print p.contains('w')
# p.delete()
# print p.contains('w')