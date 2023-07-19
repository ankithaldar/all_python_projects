#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Reward as Balance Sheet'''


# imports
from dataclasses import dataclass

#    script imports
# imports


# constants
# constants


# classes
@dataclass
class Rewards:
  '''Reward as Balance Sheet'''

  manufacturing: int = 0 # manufacturing in time
  coins: int = 0 # coins in coins

  def total(self) -> int:
    return self.manufacturing + self.coins

  def __add__(self, other):
    return Rewards(self.manufacturing + other.manufacturing, self.coins + other.coins)

  def __sub__(self, other):
    return Rewards(self.manufacturing - other.manufacturing, self.coins - other.coins)

  def __repr__(self):
    return f'{self.manufacturing+self.coins} ({self.manufacturing} {self.coins})'

  def __radd__(self, other):
    if other == 0:
      return self
    else:
      return self.__add__(other)


# classes
