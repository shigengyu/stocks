'''
Created on 24 May 2015

@author: Univer
'''

from common.logging import Logger
from cassandra.cluster import Cluster

class CassandraSession(object):
    '''
    classdocs
    '''
    host = "server.jingyusoft.com"
    keyspace = "stocks"
    logger = Logger.get_logger(__name__)

    def __init__(self):
        '''
        Constructor
        '''
    
    def connect(self):
        self.cluster = Cluster(contact_points = [CassandraSession.host])
        self.session = self.cluster.connect(CassandraSession.keyspace)
    
    def disconnect(self):
        self.cluster.shutdown()
        
    def execute(self, query, parameters = None, trace = False):
        return self.session.execute(query, parameters = parameters, trace = trace)
