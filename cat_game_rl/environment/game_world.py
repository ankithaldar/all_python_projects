#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Cat Game World'''


# imports
from collections import Counter

#    script imports
from bill_of_materials import BillOfMaterials
from game_clock import GameClock
from game_economy import GameEconomy
from item_facility import ItemFacility
from utils import (BASE_ITEMS, BATCH_SIZE, GAME_TYPE, INIT_ECONOMY,
                   ITEM_DEFAULTS, TARGET_ITEM_COUNTS, get_total_counts,
                   parse_materials, write_to_log, yaml_reader)

# imports


# constants
# constants


# classes
class CatGameWorld:
  '''Cat Game World'''

  def __init__(self):
    self.economy = GameEconomy()
    self.clock = GameClock()
    self.item_facilities = {}

  def check_presents(self):
    if self.clock.time % 5 == 0:
      self.economy.update_coins(gained_coins=210)
      write_to_log(f'{self.clock.time} | Present Collected | Coins: {self.economy.coins}')

  def check_terminate_condition(self):
    return all(
      self.item_facilities[item].total_crafted_count >= self.item_facilities[item].total_target_count
      for item in self.item_facilities
    )

  def action_per_time_step(self):
    self.clock.tick()
    self.check_presents()

    write_to_log(f'{self.clock.time} | Coins: {self.economy.coins} | stash: {self.economy.items_in_stash}')

# classes


# functions
# load initial economy conditions
def load_init_economy(world: CatGameWorld) -> CatGameWorld:
  init_economy_dict = yaml_reader(INIT_ECONOMY)

  # load stating coins
  world.economy.coins = init_economy_dict['coins']
  # load starting materials
  world.economy.items_in_stash = Counter(
    parse_materials(init_economy_dict['materials'])
  )

  return world


# load item facilities for handle crafting
def load_item_facilities(world: CatGameWorld) -> CatGameWorld:
  # fix fanal world targets for crafting
  targets = parse_materials(
    yaml_reader(TARGET_ITEM_COUNTS)['materials']
  )

  items_full_targets = get_total_counts(targets)
  items_crafted = get_total_counts(world.economy.items_in_stash)

  # adding item facilities
  item_defaults = yaml_reader(ITEM_DEFAULTS)

  for each in item_defaults['materials']:
    world.item_facilities[each['item_name']] = ItemFacility(
      name=each['item_name'],
      bom=BillOfMaterials(
        inputs=Counter(each['req_unit_raw']),
        init_cost=each['init_cost'],
        req_time=each['time'] if GAME_TYPE == 'Normal' else 1
      ),
      target_count=targets.get(each['item_name'], 0),
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



# world Builder
def worldbuilder_create():
  world = CatGameWorld()

  # load init economy
  world = load_init_economy(world=world)

  # load item facilities and its BOMS
  world = load_item_facilities(world=world)

  return world
# functions


# main
def main():
  world = worldbuilder_create()

  final_reward = []

  while not world.check_terminate_condition():
    world.action_per_time_step()
    rewards = dict()
    for item, _ in world.item_facilities.items():
      rewards[item] = world.item_facilities[item].action_per_time_step(BATCH_SIZE[item])

    final_reward.append(sum(rewards.values()))

  print(
    world.clock.time,
    world.economy.items_in_stash,
    sum(final_reward)
  )


# if main script
if __name__ == '__main__':
  main()
