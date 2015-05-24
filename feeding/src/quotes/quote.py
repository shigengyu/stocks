'''
Created on 23 May 2015

@author: Univer
'''

import unittest, time
from datetime import datetime

class EodQuote(object):
    '''
    classdocs
    '''
    
    def __init__(self, symbol, date, open_, high, low, close, volume):
        '''
        Constructor
        '''
        self.symbol = symbol
        self.date = datetime.utcfromtimestamp(time.mktime(datetime.strptime(date, "%Y-%m-%d").date().timetuple()))
        self.open = float(open_)
        self.high = float(high)
        self.low = float(low)
        self.close = float(close)
        self.volume = int(volume)

    def __str__(self, *args, **kwargs):
        return "[%s] @ %s - open = %.2f, close = %.2f, high = %.2f, low - %.2f" % (self.symbol, self.date, self.open, self.close, self.high, self.low)

class YahooEodQuote(EodQuote):
    
    def __init__(self, symbol, date, open_, high, low, close, volume, adj_close):
        super().__init__(symbol, date, open_, high, low, close, volume)
        self.adj_close = float(adj_close)

    @staticmethod
    def from_line(symbol, line):
        elements = line.split(",")
        return YahooEodQuote(symbol, elements[0], elements[1], elements[2], elements[3], elements[4], elements[5], elements[6])


class CtxEodQuote(EodQuote):
    
    def __init__(self, symbol, date, open_, high, low, close, volume, amount):
        super().__init__(symbol, date[:10], open_, high, low, close, volume)
        self.amount = amount

    @staticmethod
    def from_json(json):
        ''


class EodQuoteTests(unittest.TestCase):
    
    def test_from_line(self):
        quote = EodQuote.from_line("300460.SZ", "2015-05-22,88.80,96.57,86.10,96.57,22140100")
        assert quote.date == "2015-05-22"
        assert quote.open == "88.80"
        assert quote.high == "96.57"
        assert quote.low == "86.10"
        assert quote.close == "96.57"
        assert quote.volume == "22140100"
        