'''
Created on 23 May 2015

@author: Univer
'''

import sys, os, inspect
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
print("cmd_folder = %s", cmd_folder)
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

if __name__ == '__main__':
    from quotes import quote_feeder
    feeder = quote_feeder.QuoteFeeder(sys.argv[1])
    feeder.fetch_all()