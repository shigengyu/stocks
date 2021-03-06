'''
Created on 23 May 2015

@author: Univer
'''

import http.client
import unittest
import json
import re
import collections
from common.logging import Logger
from common.cassandra import CassandraSession

class Symbols(object):
    
    '''
    classdocs
    '''
    logger = Logger.get_logger(__name__)
    CtxStock = collections.namedtuple("CtxStock", ["symbol", "name", "short_symbol"], verbose = False)

    def __init__(self):
        '''
        Constructor
        '''
    
    @staticmethod
    def fetch_all_ctx_stocks():
        try:
            Symbols.logger.info("Creating connection to ctxalgo.com ...")
            conn = http.client.HTTPConnection("ctxalgo.com", timeout = 5000)
            Symbols.logger.info("Fetching stock list from /api/stocks ...")
            conn.request("GET", "/api/stocks")
            response = conn.getresponse().read().decode("utf-8")
            Symbols.logger.debug("Response size is %d bytes" % len(response))
            
            stocks = []
            loaded_json = json.loads(response)
            for symbol in loaded_json:
                name = loaded_json[symbol]
                stocks.append(Symbols.CtxStock(symbol, name, symbol[2:]))
            return stocks
        except:
            return []

    @staticmethod
    def search(pattern):
        symbol = Symbols.search_from_yahoo(pattern)
        if symbol != None:
            return [symbol]
        else:
            return Symbols.search_from_sina(pattern)

    @staticmethod
    def search_from_yahoo(pattern):
        try:
            conn = http.client.HTTPConnection("d.yimg.com", timeout = 5000)
            conn.request("GET", "/aq/autoc?query=%s&region=US&lang=en-US&callback=YAHOO.util.ScriptNodeDataSource.callbacks" % pattern)
            response = conn.getresponse().read().decode("utf-8")
            matched = re.search('(?<="symbol":")[0-9]{6}.[A-Z]{2}', response)
            
            if matched != None:
                return matched.group(0)
            else:
                return None
        except:
            return None

    @staticmethod
    def get_yahoo_symbol_from_ctx_symbol(ctx_symbol):
        test_yahoo_symbols = []
        if ctx_symbol.startswith("sh"):
            test_yahoo_symbols.append(ctx_symbol[2:] + ".SH")
        elif ctx_symbol.startswith("sz"):
            test_yahoo_symbols.append(ctx_symbol[2:] + ".SZ")

        test_yahoo_symbols.append(ctx_symbol[2:] + ".SS")
        
        for test_yahoo_symbol in test_yahoo_symbols:
            try:
                conn = http.client.HTTPConnection("ichart.finance.yahoo.com")
                conn.request("GET", "/table.csv?s=%s" % test_yahoo_symbol)
                
                if conn.getresponse().read(4).decode("utf-8") == "Date":
                    return test_yahoo_symbol
            except:
                return None
            
        return None

    @staticmethod
    def search_from_sina(pattern):
        symbols = []
        try:
            conn = http.client.HTTPConnection("suggest3.sinajs.cn", timeout = 10000)
            conn.request("GET", "/suggest/type=11,12,13,14,15&key=%s" % pattern)
            response = conn.getresponse().read().decode("gbk")
            matched = re.search("\"(.*)\"", response)
            if matched != None:
                for item in matched.group(1).split(";"):
                    code = item.split(",")[3]
                    conn = http.client.HTTPConnection("finance.sina.com.cn")
                    conn.request("GET", "/realstock/company/%s/nc.shtml" % code)
                    response = conn.getresponse().read().decode("gbk")
                    search_pattern = "<span>(" + pattern + "."
                    start_index = response.index(search_pattern) + len("<span>(")
                    symbol = response[start_index:start_index + len(pattern) + 3]
                    symbols.append(symbol)
            return symbols
        except:
            return []

    @staticmethod
    def insert_symbol_mapping(cassandra_session, ctx_symbol, yahoo_symbol, name, short_symbol):
        cassandra_session.execute("insert into symbols (ctx_symbol, yahoo_symbol, name, short_symbol, update_timestamp) values (%s, %s, %s, %s, dateof(now()))",
                                  (ctx_symbol, yahoo_symbol, name, short_symbol))

class SymbolsTests(unittest.TestCase):
    
    def test_fetch_all_ctx_stocks(self):
        result = Symbols.fetch_all_ctx_stocks()
        for ctx_stock in result:
            print("%s -> %s" % (ctx_stock.symbol, Symbols.get_yahoo_symbol_from_ctx_symbol(ctx_stock.symbol)))
        Symbols.logger.info("%d stocks returned from CoreTX" % len(result))
        assert len(result) > 0
     
    def test_search_from_yahoo(self):
        result_600339 = Symbols.search("600339")[0]
        assert result_600339 == "600339.SS"
        
        result_600399 = Symbols.search("600399")[0]
        assert result_600399 == "600399.SS"
    
    def test_search_from_sina(self):
        pattern = "000807"
        result = Symbols.search_from_sina(pattern);
        assert len(result) == 2
        assert result[0] == "000807.SZ"
        assert result[1] == "000807.SH"
        
    @unittest.skip
    def test_insert_symbol_mapping(self):
        cassandra_session = CassandraSession()
        try:
            cassandra_session.connect()
            Symbols.insert_symbol_mapping(cassandra_session, "sh600399", "600399.SS", None, "600399")
        finally:
            cassandra_session.disconnect()