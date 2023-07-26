# Copyright (C) 2023 EDF R&D
"""
Simple use of managers.
"""
import wip_advanced_qocq as qocq

def fonction_libre(x):
  return x * 42

@qocq.atomic_task
def s(a, b):
  ret = a + b
  return ret

@qocq.atomic_task
def p(a, b):
  ret = a * b
  return ret

@qocq.atomic_task
def sp(a, b):
  rs = s(a, b)
  t = fonction_libre(a+b)
  return rs, t

@qocq.atomic_task
def rapport(file_path, x, y, z, t):
  with open(file_path, "w") as f:
    f.write("x={}, y={}, z={}, t={}".format(x,y,z,t))

@qocq.composed_task
def calcul(x):
  v = fonction_libre(x)
  x, y = sp(v, 10)
  z = s(3,2)
  t = p(z, v)
  return x, y, z, t

@qocq.composed_task
def main():
  x, y, z, t = calcul(2)
  rapport("rapport.txt", x, y, z, t)
  #x = calcul(2)
  ##rapport("rapport.txt", x, x, x, x)

if __name__ == '__main__':
  qocq.init()
  qocq.run(main)
  qocq.close()
