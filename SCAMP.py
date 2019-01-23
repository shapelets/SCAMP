import pyscamp
import random
size = 1000000
mp = pyscamp.PiecewiseMP(100, 3, 1) 

for i in range(0,4):
  x = [random.random() for i in range(size)]
  print(len(x))
  mp.add_data(x)
  mp.update_mp()
mp.write_mp()
