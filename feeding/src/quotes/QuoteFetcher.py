'''
Created on 23 May 2015

@author: Univer
'''

import os
import http.client
import unittest
import tempfile
from common.Logger import Logger
from symbols.Symbols import Symbols

class QuoteFetcher(object):
    '''
    classdocs
    '''
    logger = Logger.get_logger(__name__)

    def __init__(self, folder):
        '''
        Constructor
        '''
        if not os.path.exists(folder):
            os.makedirs(folder)
        self.folder = folder

    def fetch(self, symbol):
        QuoteFetcher.logger.info("Fetching quotes for symbol %s ..." % symbol)
        conn = http.client.HTTPConnection("ichart.finance.yahoo.com")
        conn.request("GET", "/table.csv?s=%s" % symbol)
        response_body = conn.getresponse().read().decode("utf-8")
        
        file_name = self.folder + os.path.sep + symbol
        
        'Save response to file if valid (starts with "Date")'
        if response_body.startswith("Date"):
            try:
                text_file = open(file_name, "w")
                text_file.write(response_body)
                QuoteFetcher.logger.info("Quotes for %s saved to file %s" % (symbol, file_name))
                return file_name
            finally:
                if not text_file == None:
                    text_file.close()
        else:
            QuoteFetcher.logger.warn("Ignoring invalid quote response on symbol %s" % symbol)
            return None

    def fetch_all(self):
        for stock in Symbols.fetch_stock_list():
            QuoteFetcher.logger.info("Searching symbol with pattern %s" % stock)
            symbol = Symbols.search(stock)
            if (symbol != None):
                self.fetch(symbol)
            else:
                QuoteFetcher.logger.warn("Failed to find symbol with pattern %s" % stock)
        
class QuoteFetcherTest(unittest.TestCase):
    
    def test_fetch(self):
        symbol = "600399.SS"
        fetcher = QuoteFetcher(tempfile.gettempdir() + os.path.sep + "quotes")
        file_name = fetcher.fetch(symbol)
        assert file_name != None
        assert os.path.exists(file_name)
        os.remove(file_name)