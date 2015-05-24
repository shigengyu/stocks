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
    from quotes.quote_loader import CtxQuoteLoader
    from common.logging import Logger
    
    logger = Logger.get_logger(__name__)
    loader = CtxQuoteLoader()
    
    try:
        loader.connect()
        folder = sys.argv[1]
        for file in os.listdir(folder):
            symbol = file
            file_name = os.path.realpath(os.path.abspath(os.path.join(folder, file)))
            eod_quotes = CtxQuoteLoader.load_from_file(file_name)
            count = 0
            for eod_quote in eod_quotes:
                loader.insert_eod_quote(eod_quote)
                ++count
            logger.info("Loaded %d quotes for symbol %s" % (count, symbol))
    finally:
        loader.disconnect()