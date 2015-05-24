'''
Created on 23 May 2015

@author: Univer
'''

import sys

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Missing EOD quote folder")
        exit
        
    from quotes import quote_feeder
    ctx_feeder = quote_feeder.CtxQuoteFeeder(sys.argv[1])
    ctx_feeder.fetch_all()
    
    yahoo_feeder = quote_feeder.YahooQuoteFeeder(sys.argv[1])
    yahoo_feeder.fetch_all()
    
    