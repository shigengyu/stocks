'''
Created on 23 May 2015

@author: Univer
'''

import sys, os, inspect

current_file = inspect.getfile(inspect.currentframe())
parent_dir = os.path.join(os.path.split(current_file)[0], os.path.pardir)
parent_folder = os.path.realpath(os.path.abspath(parent_dir))
print("parent_folder = %s", parent_folder)
if parent_folder not in sys.path:
    sys.path.insert(0, parent_folder)

if __name__ == '__main__':
    from quotes import quote_feeder
    feeder = quote_feeder.QuoteFeeder(sys.argv[1])
    feeder.fetch_all()