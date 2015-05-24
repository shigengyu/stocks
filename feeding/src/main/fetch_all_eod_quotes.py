'''
Created on 23 May 2015

@author: Univer
'''

import sys, os, inspect

current_file = inspect.getfile(inspect.currentframe())
parent_dir = os.path.join(os.path.split(current_file)[0], os.path.pardir)
parent_folder = os.path.realpath(os.path.abspath(parent_dir))
if parent_folder not in sys.path:
    sys.path.insert(0, parent_folder)

if __name__ == '__main__':
    if len(sys.argv) < 3 or (sys.argv[2] != "yahoo" and sys.argv[2] != "ctx"):
        print("Usage: python3 fetch_all_eod_quotes.py /projects/stocks/data/eod_quotes [ctx|yahoo]")
        exit
    
    from quotes import quote_feeder
    if sys.argv[2] == "yahoo":
        yahoo_feeder = quote_feeder.YahooQuoteFeeder(sys.argv[1])
        yahoo_feeder.fetch_all()
        
    if sys.argv[2] == "ctx":
        ctx_feeder = quote_feeder.CtxQuoteFeeder(sys.argv[1])
        ctx_feeder.fetch_all()
