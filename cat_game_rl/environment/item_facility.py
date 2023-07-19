#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''item Facilities'''


# imports
from dataclasses import dataclass

#    script imports
from bill_of_materials import BillOfMaterials
from game_clock import GameClock
from game_economy import GameEconomy
from manufacturing_unit import ManufacturingUnit
from rewards import Rewards

# imports


# constants
# constants


# classes
@dataclass
class ItemFacility:
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
    self.current_stash = self.get_current_count_in_stash()
    self.manufacturing = ManufacturingUnit(self)
    # self.define_item_production_level()

  # def define_item_production_level(self):
  #   if len(self.sources) == 0:
  #     self.crafting_level = 1
  #   else:
  #     self.crafting_level = max([i.crafting_level for i in self.sources]) + 1

  def action_per_time_step(self, control):
    self.rewards = Rewards()
    if self.total_target_count > self.total_crafted_count:
      self.manufacturing.action_per_time_step(control)
    else:
      self.rewards = Rewards(manufacturing=self.bom.req_time)
    self.current_stash = self.get_current_count_in_stash()

    return self.rewards

  def get_current_count_in_stash(self) -> int:
    return self.game_economy.items_in_stash[self.name]


# classes
