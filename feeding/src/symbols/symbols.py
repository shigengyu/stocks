'''
Created on 23 May 2015

@author: Univer
'''

import http.client
import unittest
import json
import re
from common.Logger import Logger

class Symbols(object):
    
    '''
    classdocs
    '''
    logger = Logger.get_logger(__name__)

    def __init__(self):
        '''
        Constructor
        '''
    
    @staticmethod
    def fetch_stock_list():
        Symbols.logger.info("Creating connection to ctxalgo.com ...")
        conn = http.client.HTTPConnection("ctxalgo.com")
        Symbols.logger.info("Fetching stock list from /api/stocks ...")
        conn.request("GET", "/api/stocks")
        response = conn.getresponse().read().decode("utf-8")
        Symbols.logger.debug("Response size is %d bytes" % len(response))
        
        lines = json.loads(response)
        stocks = []
        for item in lines:
            stocks.append(item[2:])
        return stocks

    @staticmethod
    def search(pattern):
        conn = http.client.HTTPConnection("d.yimg.com")
        conn.request("GET", "/aq/autoc?query=%s&region=US&lang=en-US&callback=YAHOO.util.ScriptNodeDataSource.callbacks" % pattern)
        response = conn.getresponse().read().decode("utf-8")
        matched = re.search('(?<="symbol":")[0-9]{6}.[A-Z]{2}', response)
        
        if matched != None:
            return matched.group(0)
        else:
            return None

class SymbolsTests(unittest.TestCase):
    
    def test_fetch_list(self):
        result = Symbols.fetch_stock_list()
        Symbols.logger.info("%d stocks returned" % len(result))
        assert len(result) > 0
        
    def test_search(self):
        pattern = "600339"
        assert Symbols.search(pattern) == "600339.SS"