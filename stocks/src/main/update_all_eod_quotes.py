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
    from quotes.updater import QuoteUpdater
    from common.logging import Logger
    from datetime import datetime
    
    logger = Logger.get_logger(__name__)
    updater = QuoteUpdater()
    
    start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date().strftime("%Y-%m-%d")
    
    try:
        updater.connect()
        updater.update_all_quotes(start_date)
    finally:
        updater.disconnect()