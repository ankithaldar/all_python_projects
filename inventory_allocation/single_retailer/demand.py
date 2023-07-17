#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''historical demand'''


# imports
import numpy as np
import matplotlib.pyplot as plt
#   script imports
# imports


# constants
np.random.seed(0)
# constants


# functions
def gen_demand(loc, scale):
  return np.round(max(np.random.normal(loc, scale), 0))

def create_demand():
  demand_hist = []
  # generate demand for 52 weeks
  for _ in range(52):
    # generate demand for monday to thursday
    for _ in range(4):
      demand_hist.append(gen_demand(3, 1.5))
    # generate demand for friday
    demand_hist.append(gen_demand(6, 1))
    # generate demand for satutday & Sunday
    for _ in range(2):
      demand_hist.append(gen_demand(12, 2))

  return demand_hist

# functions


# main
def main():
  demand_hist = create_demand()
  plt.hist(demand_hist)


# if main script
if __name__ == '__main__':
  main()




