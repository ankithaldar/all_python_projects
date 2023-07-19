#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Game Clock'''


# imports
#    script imports
# imports


# constants
# constants


# classes
class GameClock:
  '''Clock to track time movements'''

  def __init__(self) -> None:
    self.__time = 0

  def tick(self) -> None:
    self.__time += 1

  @property
  def time(self) -> int:
    return self.__time


# classes
