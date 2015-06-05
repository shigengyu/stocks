'''
Created on 5 Jun 2015

@author: Univer
'''

import sys, os, inspect

current_file = inspect.getfile(inspect.currentframe())
parent_dir = os.path.join(os.path.split(current_file)[0], os.path.pardir)
parent_folder = os.path.realpath(os.path.abspath(parent_dir))
if parent_folder not in sys.path:
    sys.path.insert(0, parent_folder)

if __name__ == '__main__':
    from symbols.symbols import Symbols
    from common.cassandra import CassandraSession
    from common.logging import Logger
    
    logger = Logger.get_logger(__name__)
    
    result = Symbols.fetch_all_ctx_stocks()
    cassandra_session = CassandraSession()
    try:
        cassandra_session.connect()
        for ctx_stock in result:
            yahoo_symbol = Symbols.get_yahoo_symbol_from_ctx_symbol(ctx_stock.symbol)
            Symbols.insert_symbol_mapping(cassandra_session, ctx_stock.symbol, yahoo_symbol, ctx_stock.name, ctx_stock.short_symbol)
            logger.info("%s -> %s %s" % (ctx_stock.symbol, yahoo_symbol, ctx_stock.name))
        Symbols.logger.info("Updated %d symbols from CoreTX" % len(result))
    finally:
        cassandra_session.disconnect()
