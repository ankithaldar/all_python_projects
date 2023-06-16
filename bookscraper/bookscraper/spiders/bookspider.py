#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''spider for books.toscrape'''


#imports
import scrapy

#   script imports
#imports


# classes
class BookspiderSpider(scrapy.Spider):
  '''spider for books.toscrape'''

  name = 'bookspider'
  allowed_domains = ['books.toscrape.com']
  start_urls = ['http://books.toscrape.com']

  def parse(self, response):
    pass

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
