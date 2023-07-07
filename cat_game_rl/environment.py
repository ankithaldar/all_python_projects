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

  def __init__(self, item) -> None:
    self.item = item
    self.batch_item_count = 0

  def delete_attributes(self):
    del self.start_time
    del self.end_time
    del self.batch_item_count

  # actions that need to be taken when ending crafting
  def stop_crafting(self):
    # update stash on crafting completion
    self.item.game_economy.update_stash(
      gained_stash={self.item.name: self.batch_item_count}
    )

    # keep total crafted count
    self.item.total_crafted_count += self.batch_item_count

    # mark unit as free for crafting
    self.item.is_crafting = False

    # reset unit batch
    self.batch_item_count = 0

  def start_crafting(self):
    # goes through the batch_size and puts each item on crafting one at a time
    for _ in range(self.batch_size):
      # check coin available
      coins_available = self.check_crafting_sufficient_coins()

      # check whether inputs are available in common stash
      sources_available = self.check_crafting_sufficient_inputs()

      if coins_available and all(sources_available):
        # update coins
        self.update_coins_when_start_crafting()

        # update input inventory
        self.update_inputs_when_start_crafting()

        # add item count
        self.batch_item_count += 1

        if self.batch_item_count == 1:
          self.update_crafting_parameters_and_flags()

        self.update_source_current_stash()

      else:
        break


  # check if crafting start is possible
  def check_craft_start_posibility(self):
    start_crafting = False
    if self.check_sources_as_base_items():
      start_crafting = self.check_sources_base_items_start_crafting()
    else:
      start_crafting = True # self.batch_item_count < self.batch_size

    return start_crafting

  # check if item in string, wood, metal, bronze, amethyst and orb
  def check_sources_as_base_items(self):
    return len(self.item.sources) == 0

  # check for source base item start crafting
  def check_sources_base_items_start_crafting(self):
    return (
      self.batch_item_count == 0
      and
      not self.item.is_crafting
    )

  # check if current time is same as end time for the unit
  def check_crafting_end_time(self):
    return self.end_time == self.item.clock.time

  # checking if required coins for the item count is available
  def check_crafting_sufficient_coins(self):
    return self.item.game_economy.coins >= self.get_crafting_unit_cost()

  # checking if required inputs for the item count is available
  def check_crafting_sufficient_inputs(self):
    sources_available =[]
    if len(self.item.sources) == 0:
      return [True]

    for input_sources in self.item.sources:
      if self.item.bom.inputs.get(input_sources.name) <= input_sources.current_stash:
        sources_available.append(True)
      else:
        sources_available.append(False)

    return sources_available

  # get to be crafted unit cost
  def get_crafting_unit_cost(self):
    # check coins for next count in the batch
    return self.item.bom.init_cost * (
      1 + 0.5 * ((self.batch_item_count + 1) - 1)
    )

  # update current stash for sources
  def update_source_current_stash(self):
    for input_sources in self.item.sources:
      input_sources.current_stash = input_sources.get_current_count_in_stash()

  # update coins when start crafting
  def update_coins_when_start_crafting(self):
    self.item.game_economy.update_coins(
      used_coins=self.get_crafting_unit_cost()
    )

  # update inputs when start crafting
  def update_inputs_when_start_crafting(self):
    self.item.game_economy.update_stash(
      used_stash=self.item.bom.inputs
    )

  # update crafting parameters and flags
  def update_crafting_parameters_and_flags(self):
    # with 1st item crafting start the machine gets involved
    self.start_time = self.item.clock.time
    # flag is_crafting
    self.item.is_crafting = True
    # expected end time
    self.end_time = self.start_time + self.item.bom.req_time

  def act(self, control=0) -> None:
    if control > 0:
      self.batch_size = control

    if self.item.is_crafting:
      if self.check_crafting_end_time():
        self.stop_crafting()

    if self.item.total_crafted_count < self.item.target_count:
      # start or restart crafting again
      if self.check_craft_start_posibility():
        self.start_crafting()
    else:
      # when total crafting ends for that item
      self.delete_attributes()


# ==============================================================================
@dataclass
class Items:
  '''Item Class to maintain transaction and manufacturing of items'''

  name: str
  bom: BillOfMaterials
  target_count: int
  total_crafted_count: int
  sources: list
  game_economy: GameEconomy
  clock: GameClock

  def __post_init__(self) -> None:
    self.is_crafting = False
    self.current_stash = self.get_current_count_in_stash()
    self.manufacturing = ManuFacturingUnit(self)

  def get_current_count_in_stash(self) -> int:
    return self.game_economy.items_in_stash[self.name]

  def act(self, control):
    if self.target_count > self.total_crafted_count:
      self.manufacturing.act(control)
    self.current_stash = self.get_current_count_in_stash()


# ==============================================================================
class GameWorld:
  '''Cat Game RL World'''

  def __init__(self) -> None:
    self.economy = GameEconomy()
    self.clock = GameClock()
    self.item_facilities = {}

  def check_presents(self) -> None:
    if self.clock.time % 5 == 0:
      self.economy.update_coins(gained_coins=210)

  def check_terminate_condition(self) -> bool:
    return all(
      self.item_facilities[item].total_crafted_count >= self.item_facilities[item].target_count
      for item in self.item_facilities
    )

  def act(self):
    self.check_presents()
    for item in self.item_facilities:
      self.item_facilities[item].act(BATCH_SIZE[item])

    self.clock.tick()


# ==============================================================================
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
  items_target = get_total_counts(
    parse_materials(yaml_reader(TARGET_ITEM_COUNTS)['materials'])
  )
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
      target_count=items_target[each['item_name']],
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
    world.clock.time
    # [(
    #   k,
    #   world.item_facilities[k].manufacturing.waittime.coins,
    #   world.item_facilities[k].manufacturing.waittime.inputs
    # )
    # for k in world.item_facilities.keys()
    # ]
  )
# functions


# main
def main():
  run_basic_simulation()
  pass


# if main script
if __name__ == '__main__':
  main()
