'''
Created on 23 May 2015

@author: Univer
'''

import os
import json
import unittest
from quotes.quote import CtxEodQuote, YahooEodQuote
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
        eod_quotes = []
        with open(file_name, "r") as quote_file:
            file_content = quote_file.read().replace(os.linesep, "")
            loaded_json = json.loads(file_content)
            for symbol in loaded_json:
                arrays = loaded_json[symbol]
                for index, value in enumerate(arrays["dates"]):
                    date = arrays["dates"][index]
                    open_ = arrays["opens"][index]
                    high = arrays["highs"][index]
                    low = arrays["lows"][index]
                    close = arrays["closes"][index]
                    volume = arrays["volumes"][index]
                    amount = arrays["amounts"][index]
                    eod_quotes.append(CtxEodQuote(symbol, date, open_, high, low, close, volume, amount))
        return eod_quotes


class QuoteLoaderTests(unittest.TestCase):
    
    def test_connect(self):
        quote_loader = QuoteLoader()
        
        try:
            quote_loader.connect()
            assert quote_loader.session != None
        finally:
            quote_loader.disconnect()

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
    
    def test_load_from_file(self):
        file_name = "G:\\quotes\\300460.SZ"
        eod_quotes = YahooQuoteLoader.load_from_file(file_name)
        QuoteLoader.logger.info("Listing EOD quotes from file %s" % file_name)
        for eod_quote in eod_quotes:
            print(eod_quote)
            

class CtxQuoteLoaderTests(unittest.TestCase):
    
    def test_load_from(self):
        file_name = "H:\\Temp\\eod_quotes\\ctx\\sh600375"
        eod_quotes = CtxQuoteLoader.load_from_file(file_name)
        for eod_quote in eod_quotes:
            print(eod_quote)
