'''
Created on 23 May 2015

@author: Univer
'''

import os
import http.client
import unittest
import time
import tempfile
import abc
from common.logging import Logger
from symbols.symbols import Symbols
from common.cassandra import CassandraSession

class QuoteFeeder(object):
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

    def get_file_name_by_symbol(self, symbol):
        return self.folder + os.path.sep + symbol

    @abc.abstractmethod
    def download_quotes(self, symbol, skip_existing):
        return None

    def fetch(self, symbol, skip_existing = True):
        QuoteFeeder.logger.debug("Fetching quotes for symbol %s ..." % symbol)
        file_name = self.get_file_name_by_symbol(symbol)
        
        if skip_existing and os.path.exists(file_name):
            QuoteFeeder.logger.info("Quotes for %s already exists in %s" % (symbol, file_name))
        else:
            file_name = self.download_quotes(symbol, skip_existing)
            if file_name != None:
                QuoteFeeder.logger.info("Quotes for %s saved to file %s" % (symbol, file_name))
            else:
                QuoteFeeder.logger.warning("Failed to fetch quotes for %s" % symbol)

        return file_name


class YahooQuoteFeeder(QuoteFeeder):
    
    def __init__(self, folder):
        super().__init__(os.path.join(folder, "yahoo"))
    
    def download_quotes(self, symbol, skip_existing):
        conn = http.client.HTTPConnection("ichart.finance.yahoo.com")
        conn.request("GET", "/table.csv?s=%s" % symbol)
        response_body = conn.getresponse().read().decode("utf-8")
        
        file_name = self.get_file_name_by_symbol(symbol)
        
        'Save response to file if valid (starts with "Date")'
        if response_body.startswith("Date"):
            try:
                text_file = open(file_name, "w")
                text_file.write(response_body)
                return file_name
            finally:
                if not text_file == None:
                    text_file.close()
        else:
            QuoteFeeder.logger.warning("Ignoring invalid quote response on symbol %s" % symbol)
            return None


    def fetch_all(self):        
        cassandra_session = CassandraSession()        
        try:
            cassandra_session.connect()
            
            for ctx_stock in Symbols.fetch_all_ctx_stocks():
                test_yahoo_symbols = []
                if ctx_stock.symbol.startswith("sh"):
                    test_yahoo_symbols.append(ctx_stock.symbol[2:] + ".SH")
                elif ctx_stock.symbol.startswith("sz"):
                    test_yahoo_symbols.append(ctx_stock.symbol[2:] + ".SZ")
    
                test_yahoo_symbols.append(ctx_stock.symbol[2:] + ".SS")
                
                for test_yahoo_symbol in test_yahoo_symbols:
                    if self.fetch(test_yahoo_symbol) != None:
                        matched_yahoo_symbol = test_yahoo_symbol
                        break
    
                if matched_yahoo_symbol != None:
                    QuoteFeeder.logger.info("[%s] -> [%s]" % (ctx_stock.symbol, matched_yahoo_symbol))
                else:
                    QuoteFeeder.logger.info("[%s] not found in Yahoo" % ctx_stock.symbol)
                
                self.insert_symbol_mapping(cassandra_session, ctx_stock.symbol, matched_yahoo_symbol, ctx_stock.name, ctx_stock.symbol[2:])
        finally:
            cassandra_session.disconnect()

    def insert_symbol_mapping(self, cassandra_session, ctx_symbol, yahoo_symbol, name, short_symbol):
        cassandra_session.execute("insert into symbols (ctx_symbol, yahoo_symbol, name, short_symbol, update_timestamp) values (%s, %s, %s, %s, dateof(now()))",
                                  (ctx_symbol, yahoo_symbol, name, short_symbol))


class CtxQuoteFeeder(QuoteFeeder):
    
    def __init__(self, folder):
        super().__init__(os.path.join(folder, "ctx"))

    def download_quotes(self, symbol, skip_existing):
        conn = http.client.HTTPConnection("ctxalgo.com")
        conn.request("GET", "/api/ohlc/%s?start-date=%s&end-date=%s" % (symbol, "2010-01-01", time.strftime("%Y-%m-%d")))
        response_body = conn.getresponse().read().decode("utf-8")
        
        file_name = self.get_file_name_by_symbol(symbol)
        
        if skip_existing and os.path.exists(file_name):
            QuoteFeeder.logger.info("Quotes for %s already exists in %s" % (symbol, file_name))
            return file_name
        
        try:
            text_file = open(file_name, "w")
            text_file.write(response_body)
            return file_name
        finally:
            if text_file != None:
                text_file.close()

    def fetch_all(self):        
        for ctx_stock in Symbols.fetch_all_ctx_stocks():
            self.fetch(ctx_stock.symbol, skip_existing = True)


class YahooQuoteFeederTest:
    
    def test_fetch(self):
        symbol = "600399.SS"
        fetcher = YahooQuoteFeeder(tempfile.gettempdir() + os.path.sep + "eod_quotes")
        file_name = fetcher.fetch(symbol) 
        assert file_name != None
        assert os.path.exists(file_name)
        os.remove(file_name)
    
    def test_insert_symbol_mapping(self):
        cassandra_session = CassandraSession()
        yahoo_quote_fetcher = YahooQuoteFeeder(folder = tempfile.gettempdir() + os.path.sep + "yahoo_quotes")
        try:
            cassandra_session.connect()
            yahoo_quote_fetcher.insert_symbol_mapping(cassandra_session, "sh600399", "600399.SS", None, "600399")
        finally:
            cassandra_session.disconnect()

    def test_fetch_all(self):
        fetcher = YahooQuoteFeeder(tempfile.gettempdir() + os.path.sep + "eod_quotes")
        fetcher.fetch_all()


class CtxQuoteFeederTest(unittest.TestCase):
    
    def test_fetch(self):
        symbol = "sh600399"
        fetcher = CtxQuoteFeeder(tempfile.gettempdir() + os.path.sep + "eod_quotes")
        file_name = fetcher.fetch(symbol)
        assert file_name != None
        assert os.path.exists(file_name)
        os.remove(file_name)


    def test_fetch_all(self):
        fetcher = CtxQuoteFeeder(tempfile.gettempdir() + os.path.sep + "eod_quotes")
        fetcher.fetch_all()