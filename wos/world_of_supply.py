#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Doc String for the module'''


# imports
import importlib
import numpy as np
from tqdm import tqdm as tqdm

#   script imports
import world_of_supply_environment as ws
# import world_of_supply_renderer as wsr
import world_of_supply_tools as wst

for module in [ws, wst]:
  importlib.reload(module)
# imports


# constants
# constants


# Core Simulation and logic
# Measure the simulation rate, steps/sec
eposod_len = 1000
n_episods = 10
world = ws.WorldBuilder.create()
tracker = wst.SimulationTracker(eposod_len, n_episods, world.facilities.keys())
with tqdm(total=eposod_len * n_episods) as pbar:
  for i in range(n_episods):
    world = ws.WorldBuilder.create()
    policy = ws.SimpleControlPolicy()
    for t in range(eposod_len):
      outcome = world.act(policy.compute_control(world))
      tracker.add_sample(
        i,
        t,
        world.economy.global_balance().total(),
        {k: v.total() for k, v in outcome.facility_step_balance_sheets.items()}
      )
      pbar.update(1)
tracker.render()

# Core Simulation and logic


# functions
def function_name():
  pass
# functions


# main
def main():
  pass


# if main script
if __name__ == '__main__':
  main()
