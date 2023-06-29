#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Environment'''


# imports
from abc import ABC
from collections import Counter #, ChainMap
from dataclasses import dataclass # , field
from functools import reduce
from pathlib import Path
# from typing import Dict, List
import yaml

try:
  from yaml import CLoader
except ImportError:
  from yaml import Loader as CLoader
#   script imports
# imports


# constants
BASE_ITEMS = ['tree', 'cotton', 'rock', 'quartz']

PATH_CWD = Path.cwd() if 'cat_game_rl' in str(
  Path.cwd()
).split('/') else Path.cwd() / 'cat_game_rl'

INIT_ECONOMY = PATH_CWD / 'loaders'/'init_game_economy.yml'
ITEM_DEFAULTS = PATH_CWD / 'loaders'/'item_default_config.yml'
TARGET_ITEM_COUNTS = PATH_CWD / 'loaders'/'crafting_target.yml'
ITEM_COUNTS = Counter()
DEBUG = True

BATCH_SIZE = {
  'string': 10,
  'wood': 10,
  'metal': 12 ,
  'ribbon': 5,
  'needles': 3,
  'sparkles': 2,
  'bronze': 3,
  'silver': 2,
  'gold': 1,
  'amethyst': 3,
  'pendant': 1,
  'necklace': 1,
  'orb': 2,
  'water': 2,
  'fire': 1,
  'waterstone': 1,
  'firestone': 1,
  'elementstone': 1,
  'artifact': 1
}

GAME_TYPE = 'Normal' # Normal
# constants


# classes
class Agent(ABC):
  def act(self, control):
    pass


# ==============================================================================
@dataclass
class WaitTime:
  '''Calculate manufacturing wait times'''

  inputs_time: Counter = Counter()

  def __add__(self, other):
    return WaitTime(self.inputs_time + other.inputs_time)

  def __repr__(self):
    return f'{ {k: v for k, v in self.inputs_time} }'

# ==============================================================================
@dataclass
class BillOfMaterials:
  '''handle bill of materials'''

  inputs: Counter  # (product_id -> quantity per lot)
  init_cost: int
  req_time: int
  output_lot_size: int = 1
  # base_items: list[str] = field(default_factory=lambda: BASE_ITEMS)

  def input_batch_counts(self, batch_size):
    return Counter({k: v*batch_size for k, v in self.inputs.items()})

  def input_item_batch_count(self, item, batch_size):
    return self.inputs[item] * batch_size


# ==============================================================================
class GameClock:
  '''Clock to track time movements'''

  def __init__(self) -> None:
    self.__time = 0

  def tick(self) -> None:
    self.__time += 1

  @property
  def time(self) -> int:
    return self.__time


# ==============================================================================
class GameEconomy:
  '''Game Economy - Coins & item stash'''

  def __init__(self) -> None:
    self.coins = 0
    self.items_in_stash = Counter()

    self.used_coins = 0
    self.gained_coins = 0

  def update_coins(self, used_coins=0, gained_coins=0) -> None:
    self.coins += gained_coins - used_coins
    self.used_coins += used_coins
    self.gained_coins += gained_coins

  def update_stash(self, used_stash=None, gained_stash=None) -> None:
    if used_stash is not None:
      self.items_in_stash.subtract(used_stash)

    if gained_stash is not None:
      self.items_in_stash.update(gained_stash)


# ==============================================================================
class ManuFacturingUnit(Agent):
  '''Unit for crafting individual items'''

  # control is batch_size only

  def __init__(self, item) -> None:
    self.item = item
    self.last_run_batch_size = 0

  def manage_crafting(self):
    # check if batch count of crafts is required to craft
    if self.item.total_crafted_count + self.batch_size > self.item.total_target_count:
      self.batch_size = self.item.total_target_count - self.item.total_crafted_count

    # define current running batch size
    self.last_run_batch_size = self.batch_size
    self.batch_cost = self.get_batch_cost()

    # check if batch size to be crafted is possible to craft
    if self.check_possible_manufacture():
      self.start_crafting()
      return WaitTime()
    else:
      # add waittime calculation logic
      waittime = {}
      if not self.item.is_crafting:
        for k in self.item.bom.inputs:
          if (
            self.item.game_economy.items_in_stash[k]
            >=
            self.item.bom.input_item_batch_count(k, self.last_run_batch_size)
          ):
            waittime[k] = 1

      return WaitTime(Counter(waittime))

  def start_crafting(self):
    # update coins
    self.item.game_economy.update_coins(
      used_coins=self.get_batch_cost()
    )

    # update crafted account stash
    self.item.game_economy.update_stash(
      used_stash=self.item.bom.input_batch_counts(self.last_run_batch_size)
    )

    # start crafting - handle times
    self.start_time = self.item.clock.time
    self.item.is_crafting = True

    # expected end time
    self.end_time = self.start_time + self.item.bom.req_time

    # update last piece value
    self.item.last_piece_value = self.item.bom.init_cost * \
        (1 + (self.last_run_batch_size - 1) / 2)

  def check_crafting_end_time(self):
    if self.end_time == self.item.clock.time:
      self.stop_crafting()

  def stop_crafting(self):
    self.item.is_crafting = False

    self.item.game_economy.update_stash(
      gained_stash={self.item.name: self.last_run_batch_size}
    )

    self.item.total_crafted_count += self.last_run_batch_size

    self.item.last_piece_value = self.item.bom.init_cost

  def delete_attributes(self):
    del self.start_time
    del self.end_time
    del self.item.last_piece_value
    del self.last_run_batch_size
    del self.batch_cost

  def check_possible_manufacture(self):
    return (
      # checking if the units is already crafting
      not self.item.is_crafting
      and
      # checking if the coins are available
      self.check_crafting_sufficient_coins()
      and
      # checking if raw meritials are available for the batch size
      self.check_input_item_availability()
    )

  def check_crafting_sufficient_coins(self):
    return self.item.game_economy.coins >= self.get_batch_cost()

  def check_input_item_availability(self):
    return all(
      # check if input items are crafted and available for crafting
      self.item.game_economy.items_in_stash[item] >= self.input_items_batch(req)
      if item not in BASE_ITEMS else True
      for item, req in self.item.bom.inputs.items()
      #?? Note Check for a logic to use BOM input_batch_counts() instead
    )

  # get input items batch size
  def input_items_batch(self, req_unit_prev):
    return self.last_run_batch_size * req_unit_prev

  # get total batch cost
  def get_batch_cost(self) -> int:
    item_cost = [0] * self.last_run_batch_size
    for i in range(1, self.last_run_batch_size + 1):
      item_cost[i - 1] += self.item.bom.init_cost * (1 + 0.5 * (i - 1))
    return sum(item_cost)

  def act(self, control):
    self.batch_size = control
    waittime = Counter()

    # end crafting
    if self.item.is_crafting:
      self.check_crafting_end_time()

    if self.item.total_crafted_count < self.item.target_count:
      # start or restart crafting again
      waittime = self.manage_crafting()
    else:
      # when total crafting ends for that item
      self.delete_attributes()



# ==============================================================================
@dataclass
class Items:
  '''Item Class to maintain transaction and manufacturing of items'''

  name: str
  bom: BillOfMaterials
  target_count: int # final number that is needed for decor crafting
  total_target_count: int # total number of pirces needed to fulfil
  total_crafted_count: int
  sources: list
  game_economy: GameEconomy
  clock: GameClock

  def __post_init__(self):
    self.is_crafting = False
    self.last_piece_value = self.bom.init_cost
    self.manufacturing = ManuFacturingUnit(self)

  def act(self, control):
    if self.target_count > self.total_crafted_count:
      self.manufacturing.act(control)
    self.current_stash = self.game_economy.items_in_stash[self.name]


# ==============================================================================
class GameWorld:
  '''Cat Game RL World'''

  def __init__(self):
    self.economy = GameEconomy()
    self.clock = GameClock()
    self.item_facilities = {}

  def check_presents(self):
    if self.clock.time % 5 == 0:
      self.economy.update_coins(gained_coins=210)

  def check_terminate_condition(self):
    return all(
      self.item_facilities[item].total_crafted_count >= self.item_facilities[item].target_count
      for item in self.item_facilities
    )

  def act(self, control={}):
    self.clock.tick()
    self.check_presents()
    for item in self.item_facilities:
      self.item_facilities[item].act(BATCH_SIZE[item])


# classes


# functions
# Read YAML Files
def yaml_reader(file_path: Path) -> dict:
  if file_path.exists():
    with file_path.open('r') as f:
      return yaml.load(f, Loader=CLoader)
  else:
    print(f'Config file not found at {file_path.absolute()}.')
    raise FileNotFoundError


# parse dict with materials
def parse_materials(materials: list) -> dict:
  # return dict(ChainMap(*materials))
  return reduce(lambda a, b: {**a, **b}, materials)


# load initial economy conditions
def load_init_economy(world: GameWorld) -> GameWorld:
  init_economy_dict = yaml_reader(INIT_ECONOMY)

  # load stating coins
  world.economy.coins = init_economy_dict['coins']
  # load starting materials
  world.economy.items_in_stash = Counter(
    parse_materials(init_economy_dict['materials'])
  )

  return world


# Calculate total items
def read_item_defaults():
  return yaml_reader(ITEM_DEFAULTS)


def get_raw_material_count(item, counts, required_item_raw):
  for prev_item, prev_qty in required_item_raw[item].items():
    ITEM_COUNTS[prev_item] += counts * prev_qty
    if prev_item not in BASE_ITEMS:
      get_raw_material_count(prev_item, counts * prev_qty, required_item_raw)


def get_total_counts(target_items):
  # target_items = {'artifact': 1}
  item_defaults = read_item_defaults()

  required_item_raw = {}
  for each in item_defaults['materials']:
    required_item_raw[each['item_name']] = each['req_unit_raw']
    ITEM_COUNTS[each['item_name']] = 0

  # get total items to be crafted
  for item, counts in target_items.items():
    if counts > 0:
      ITEM_COUNTS[item] += counts
      if item not in BASE_ITEMS:
        get_raw_material_count(item, counts, required_item_raw)

  return Counter(ITEM_COUNTS)


# load item facilities for handle crafting
def load_item_facilities(world: GameWorld) -> GameWorld:
  # fix fanal world targets for crafting
  targets = parse_materials(
    yaml_reader(TARGET_ITEM_COUNTS)['materials']
  )

  items_full_targets = get_total_counts(targets)
  items_crafted = get_total_counts(world.economy.items_in_stash)

  # adding item facilities
  item_defaults = read_item_defaults()

  for each in item_defaults['materials']:
    world.item_facilities[each['item_name']] = Items(
      name=each['item_name'],
      bom=BillOfMaterials(
        inputs=Counter(each['req_unit_raw']),
        init_cost=each['init_cost'],
        req_time=each['time'] if GAME_TYPE == 'Normal' else 1
      ),
      target_count=targets[each['item_name']],
      total_target_count=items_full_targets[each['item_name']],
      total_crafted_count=items_crafted[each['item_name']],
      sources = [
        world.item_facilities[i] for i in each['req_unit_raw']
        if i not in BASE_ITEMS
      ],
      game_economy=world.economy,
      clock=world.clock
    )

  return world


def worldbuilder_create():
  world = GameWorld()

  # load init economy
  world = load_init_economy(world=world)

  # load item facilities and its BOMS
  world = load_item_facilities(world=world)

  return world

# tesing functions to check if everything is fine
def run_basic_simulation():
  world = worldbuilder_create()

  while not world.check_terminate_condition():
    world.act()

  print(
    world.clock.time,
    [(
      k,
      world.item_facilities[k].manufacturing.waittime.coins,
      world.item_facilities[k].manufacturing.waittime.inputs
    )
    for k in world.item_facilities.keys()
    ]
  )
# functions


# main
def main():
  run_basic_simulation()
  pass


# if main script
if __name__ == '__main__':
  main()
