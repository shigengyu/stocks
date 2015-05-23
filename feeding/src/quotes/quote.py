'''
Created on 23 May 2015

@author: Univer
'''

import unittest

class EodQuote(object):
    '''
    classdocs
    '''
    
    def __init__(self, date, open_, high, low, close, volume, adj_close):
        '''
        Constructor
        '''
        self.date = date
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.adj_close = adj_close
    
    @staticmethod
    def from_line(line):
        elements = line.split(",")
        return EodQuote(elements[0], elements[1], elements[2], elements[3], elements[4], elements[5], elements[6])
    
class EodQuoteTests(unittest.TestCase):
    
    def test_from_line(self):
        quote = EodQuote.from_line("2015-05-22,88.80,96.57,86.10,96.57,22140100,96.57")
        assert quote.date == "2015-05-22"
        assert quote.open == "88.80"
        assert quote.high == "96.57"
        assert quote.low == "86.10"
        assert quote.close == "96.57"
        assert quote.volume == "22140100"
        assert quote.adj_close == "96.57"
        