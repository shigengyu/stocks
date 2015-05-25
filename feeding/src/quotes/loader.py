'''
Created on 23 May 2015

@author: Univer
'''

import os
import unittest
from quotes.eod_quote import CtxEodQuote, YahooEodQuote
from cassandra.cluster import Cluster
from common.logging import Logger

class QuoteLoader(object):

    host = "server.jingyusoft.com"
    keyspace = "stocks"
    logger = Logger.get_logger(__name__)

    def connect(self):
        self.cluster = Cluster(contact_points = [QuoteLoader.host])
        self.session = self.cluster.connect(QuoteLoader.keyspace)
    
    def disconnect(self):
        if self.cluster != None:
            self.cluster.shutdown()

    def insert_eod_quote(self, eod_quote):
        cql = "insert into eod_quotes (symbol, date, open, high, low, close, volume) values ('%s', '%s', %.2f, %.2f, %.2f, %.2f, %d)" \
            % (eod_quote.symbol, eod_quote.date.strftime("%Y-%m-%d"), eod_quote.open, eod_quote.high, eod_quote.low, eod_quote.close, eod_quote.volume)
        self.session.execute(cql)


class YahooQuoteLoader(QuoteLoader):
    
    @staticmethod
    def load_from_file(file_name):
        quote_file = open(file_name, "r")
        symbol = file_name[file_name.rfind(os.path.sep) + 1:]
        eod_quotes = []
        for line in quote_file:
            if not line.startswith("Date"):
                quote = YahooEodQuote.from_line(symbol, line)
                eod_quotes.append(quote)
        return eod_quotes


class CtxQuoteLoader(QuoteLoader):

    @staticmethod
    def load_from_file(file_name):
        with open(file_name, "r") as quote_file:
            file_content = quote_file.read().replace(os.linesep, "")
            return CtxEodQuote.from_json(file_content)


class QuoteLoaderTests(unittest.TestCase):
    
    def test_connect(self):
        quote_loader = QuoteLoader()
        
        try:
            quote_loader.connect()
            assert quote_loader.session != None
        finally:
            quote_loader.disconnect()

    @unittest.skip
    def test_insert_eod_quote(self):
        quote_loader = QuoteLoader()
        try:
            quote_loader.connect()
            file_name = "H:\\Temp\\eod_quotes\\ctx\\sh600375"
            eod_quotes = CtxQuoteLoader.load_from_file(file_name)
            for eod_quote in eod_quotes:
                quote_loader.insert_eod_quote(eod_quote)
        finally:
            quote_loader.disconnect()

class YahooQuoteLoaderTests(unittest.TestCase):

    @unittest.skip    
    def test_load_from_file(self):
        file_name = "G:\\quotes\\300460.SZ"
        eod_quotes = YahooQuoteLoader.load_from_file(file_name)
        QuoteLoader.logger.info("Listing EOD quotes from file %s" % file_name)
        for eod_quote in eod_quotes:
            print(eod_quote)
            

class CtxQuoteLoaderTests(unittest.TestCase):

    @unittest.skip    
    def test_load_from(self):
        file_name = "H:\\Temp\\eod_quotes\\ctx\\sh600375"
        eod_quotes = CtxQuoteLoader.load_from_file(file_name)
        for eod_quote in eod_quotes:
            print(eod_quote)
