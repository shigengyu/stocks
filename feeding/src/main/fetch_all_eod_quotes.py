'''
Created on 23 May 2015

@author: Univer
'''

import sys
from quotes import quote_fetcher

if __name__ == '__main__':
    quote_fetcher = quote_fetcher.QuoteFetcher(sys.argv[1])
    quote_fetcher.fetch_all()