import wip_advanced_qocq as qocq
import time

@qocq.atomic_task
def f(x):
  import os
  print("pid:", os.getpid(), " sleep ", x)
  time.sleep(x)
  #with open("rapport-"+str(x)+".txt", "w") as f:
    #f.write("coucou!"+str(x))
  return x + x

@qocq.composed_task
def sec(x):
  f(x)

@qocq.composed_task
def main():
  t1 = f(2)
  t2 = f(t1)
  t3 = f(3)
  t4 = f(t1)
  t5 = sec(t2)

if __name__ == '__main__':
  import os
  print("main pid:", os.getpid())
  qocq.init()
  start_time = time.time()
  qocq.run(main)
  end_time = time.time()
  elapsed_time = end_time - start_time
  print("Elapsed time: ", elapsed_time)
  if elapsed_time >= 14.0 and elapsed_time <= 14.5 :
    print("ok!")
  else:
    print("ça craint!")
