'''
Created on 23 May 2015

@author: Univer
'''

import sys
from quotes import quote_loader

if __name__ == '__main__':
    loader = quote_loader.QuoteLoader()
    try:
        loader.connect()
        file_name = sys.argv[1]
        eod_quote = quote_loader.QuoteLoader.load_from_file(file_name)
        loader.insert_eod_quote(eod_quote)
    finally:
        loader.disconnect()