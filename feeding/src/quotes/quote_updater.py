'''
Created on 24 May 2015

@author: Univer
'''

import os
import http.client
import time
import json
import unittest
from quotes.quote import CtxEodQuote
from symbols.symbols import Symbols
from quotes.quote_loader import QuoteLoader
from common.logging import Logger

class QuoteUpdater(object):
    '''
    classdocs
    '''
    logger = Logger.get_logger(__name__)

    def __init__(self):
        '''
        Constructor
        '''
        self.quote_loader = QuoteLoader() 
        
    def connect(self):
        self.quote_loader.connect()
                
    def disconnect(self):
        self.quote_loader.disconnect()
    
    def update_quotes(self, symbols, start_date):
        conn = http.client.HTTPConnection("ctxalgo.com")
        url = "/api/ohlc/%s?start-date=%s&end-date=%s" % (",".join(symbols), start_date, time.strftime("%Y-%m-%d"))
        print(url)
        conn.request("GET", url)
        response_body = conn.getresponse().read().decode("utf-8")
        loaded_json = json.loads(response_body)
        
        eod_quotes = []
        
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
                
                eod_quote = CtxEodQuote(symbol, date, open_, high, low, close, volume, amount)
                eod_quotes.append(eod_quote)
                self.quote_loader.insert_eod_quote(eod_quote)

        QuoteUpdater.logger.info("Updated %d quotes for %d symbols between %s and today" % (len(eod_quotes), len(list(symbols)), start_date))

    def update_all_quotes(self, start_date):
        stocks = Symbols.fetch_all_ctx_stocks()
        chunks = [stocks[x: x + 50] for x in range(0, len(stocks), 50)]
        for chunk in chunks:
            symbols = map(lambda stock : stock.symbol, chunk)
            self.update_quotes(symbols, start_date)

class QuoteUpdaterTests(unittest.TestCase):
    
    def test_update_quotes(self):
        quote_updater = QuoteUpdater()
        try:
            quote_updater.connect()
            quote_updater.update_all_quotes("2015-05-20")
        finally:
            quote_updater.disconnect()
