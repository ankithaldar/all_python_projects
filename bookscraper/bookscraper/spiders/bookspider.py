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
    books = response.css('article.product_pod')

    for book in books:
      yield {
        'name': book.css('h3 a::text').get(),
        'price': book.css('.product_price .price_color::text').get(),
        'url': book.css('h3 a').attrib('href'),
      }

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
