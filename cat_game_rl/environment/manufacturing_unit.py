#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Manufacturing Unit'''


# imports
#    script imports
from utils import DEBUG, write_to_log

# imports


# constants
# constants


# classes
class ManufacturingUnit:
  '''Manufacturing Unit'''

  def __init__(self, item):
    super().__init__()
    self.item = item

  def action_per_time_step(self, batch_size):
    self.batch_size = batch_size

    if self.item.total_crafted_count + self.batch_size > self.item.total_target_count:
      self.batch_size = self.item.total_target_count - self.item.total_crafted_count

    # end crafting
    if self.item.is_crafting:
      if self.check_crafting_end_time():
        self.stop_crafting()


    if self.item.total_target_count > self.item.total_crafted_count:
      # start or restart crafting again
      if self.is_crafting_possible():
        self.start_crafting()
    else:
      # when total crafting ends for that item
      self.delete_attributes()

  # check if crafting end time is same as time in clock
  def check_crafting_end_time(self):
    return self.end_time == self.item.clock.time

  # stop crafting
  def stop_crafting(self):
    self.item.game_economy.update_stash(
      gained_stash={self.item.name: self.batch_size}
    )

    # keep total crafted count
    self.item.total_crafted_count += self.batch_size

    # mark unit as free for crafting
    self.item.is_crafting = False

    if DEBUG:
      write_to_log(f'{self.item.clock.time} | stopped crafting: {self.item.name} | batch_size: {self.batch_size} | current stash: {self.item.current_stash} | total crafted: {self.item.total_crafted_count}')

  # check if required resources are available for crafting
  def is_crafting_possible(self):
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
    return self.item.game_economy.coins >= self.item.bom.get_batch_cost(self.batch_size)

  def check_input_item_availability(self):
    return self.item.game_economy.items_in_stash >= self.item.bom.get_batch_input(self.batch_size)

  # start crafting
  def start_crafting(self):
    # update coins
    self.item.game_economy.update_coins(
      used_coins=self.item.bom.get_batch_cost(self.batch_size)
    )

    # update crafted account stash
    self.item.game_economy.update_stash(
      used_stash=self.item.bom.get_batch_input(self.batch_size)
    )

    # start crafting - handle times
    self.start_time = self.item.clock.time
    self.item.is_crafting = True

    # expected end time
    self.end_time = self.start_time + self.item.bom.req_time

    for inputs in self.item.sources:
      inputs.current_stash = inputs.get_current_count_in_stash()

    if DEBUG:
      write_to_log(f'{self.item.clock.time} | started crafting: {self.item.name} | batch_size: {self.batch_size} | current stash: {self.item.current_stash} | total crafted: {self.item.total_crafted_count}')

  def delete_attributes(self):
    del self.start_time
    del self.end_time
# classes

