#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Q Network'''


# imports
import torch
import torch.nn as nn
import torch.nn.functional as F
#    script imports
# imports


# classes


class QNetwork(nn.Module):
  ''' Actor (Policy) Model.'''
  def __init__(self, state_size, action_size, seed, fc1_unit=128,
         fc2_unit = 128):
    '''
    Initialize parameters and build model.
    Params
    =======
      state_size (int): Dimension of each state
      action_size (int): Dimension of each action
      seed (int): Random seed
      fc1_unit (int): Number of nodes in first hidden layer
      fc2_unit (int): Number of nodes in second hidden layer
    '''
    super(QNetwork,self).__init__() ## calls __init__ method of nn.Module class
    self.seed = torch.manual_seed(seed)
    self.fc1= nn.Linear(state_size,fc1_unit)
    self.fc2 = nn.Linear(fc1_unit,fc2_unit)
    self.fc3 = nn.Linear(fc2_unit,action_size)

  def forward(self,x):
    '''
    Build a network that maps state -> action values.
    '''
    x = F.relu(self.fc1(x))
    x = F.relu(self.fc2(x))
    return self.fc3(x)

# classes


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
