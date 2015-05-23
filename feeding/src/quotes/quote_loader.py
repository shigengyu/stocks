'''
Created on 23 May 2015

@author: Univer
'''

import sys
import os
import unittest
from quotes.quote import EodQuote
from cassandra.cluster import Cluster
from common.Logger import Logger

class QuoteLoader(object):
    '''
    classdocs
    '''
    host = "server.jingyusoft.com"
    keyspace = "stocks"
    logger = Logger.get_logger(__name__)

    def __init__(self):
        '''
        Constructor
        '''
    
    @staticmethod
    def load_from_file(file_name):
        quote_file = open(file_name, "r")
        symbol = file_name[file_name.rfind(os.path.sep) + 1:]
        eod_quotes = []
        for line in quote_file:
            if not line.startswith("Date"):
                quote = EodQuote.from_line(symbol, line)
                eod_quotes.append(quote)
        return eod_quotes
    
    def connect(self):
        self.cluster = Cluster(contact_points = [QuoteLoader.host])
        self.session = self.cluster.connect(QuoteLoader.keyspace)
    
    def disconnect(self):
        self.cluster.shutdown()
    
    def insert_eod_quote(self, eod_quote):
        cql = "insert into eod_quotes (symbol, date, open, high, low, close, volume, adj_close) values ('%s', '%s', %.2f, %.2f, %.2f, %.2f, %d, %.2f)" \
            % (eod_quote.symbol, eod_quote.date.strftime("%Y-%m-%d"), eod_quote.open, eod_quote.high, eod_quote.low, eod_quote.close, eod_quote.volume, eod_quote.adj_close)
        self.session.execute(cql)
        

if __name__ == "__main__":
    quote_loader = QuoteLoader()
    try:
        quote_loader.connect()
        file_name = sys.argv[1]
        eod_quote = QuoteLoader.load_from_file(file_name)
        quote_loader.insert_eod_quote(eod_quote)
    finally:
        quote_loader.disconnect()

class QuoteLoaderTests(unittest.TestCase):
    
    def test_load_from_file(self):
        file_name = "G:\\quotes\\300460.SZ"
        eod_quotes = QuoteLoader.load_from_file(file_name)
        QuoteLoader.logger.info("Listing EOD quotes from file %s" % file_name)
        for eod_quote in eod_quotes:
            print(eod_quote)
            
    def test_connect(self):
        quote_loader = QuoteLoader()
        quote_loader.connect()
        assert quote_loader.session != None
        
    def test_insert_cassandra(self):
        quote_loader = QuoteLoader("server.jingyusoft.com", "stocks")
        quote_loader.connect()
        file_name = "G:\\quotes\\300460.SZ"
        eod_quotes = QuoteLoader.load_from_file(file_name)
        for eod_quote in eod_quotes:
            quote_loader.insert_eod_quote(eod_quote)
