from sqlalchemy.engine import create_engine
import unittest
import pandas as pd
import datetime

class DatabaseInterface(object):

    def __init__(self, connector):
        self._connector = connector
        self._engine = self._connector.create_engine()
        self._engine.echo = True
        
    def create_connection(self):
        return DatabaseConnectionHolder(self._engine)
    
    def select(self):
        pass
    
    def select_dataframe(self, sql):
        return pd.read_sql_query(sql, self._engine)
    
    def insert_dataframe(self, dataframe, table_name):
        metadata = TableMetadata.get(self._engine, table_name)
        if metadata.identity_column != None:
            dataframe.drop(metadata.identity_column, axis=1, inplace=True)
        dataframe.to_sql(metadata.table_name, self._engine, if_exists = 'append', index = False)
    
    '''
    Merge data frame into table. staging_table_name must be provided as data frame to_sql does not yet support reusing connection
    '''
    def merge_dataframe(self, dataframe, table_name, match_columns=[], src_alias='src', dest_alias='dest', additional_columns = {}, exclude_columns = []):
        if match_columns == None or len(match_columns) == 0:
            raise DatabaseInterfaceError("Match columns must be provided if data frame does not have index")
                
        metadata = TableMetadata.get(self._engine, table_name)
        try:
            conn = self._engine.connect()
            staging_table_name = DatabaseInterface._create_staging_table_name(table_name)
            
            if metadata.identity_column != None:
                dataframe.drop(metadata.identity_column, axis=1, inplace=True)
            for column in exclude_columns:
                dataframe.drop(column, axis=1, inplace=True)
            dataframe.drop_duplicates(subset=match_columns, inplace=True)
            dataframe.to_sql(staging_table_name, conn.engine, index = False)    # current pandas version does not support reusing connection
            rowcount = conn.execute("select count(*) from %s" % staging_table_name).fetchone()[0]
            print("Inserted %d row(s) into %s" % (rowcount, staging_table_name))
            
            merge_sql = '''
            MERGE INTO %s %s
            USING %s %s
            ON %s
            WHEN MATCHED THEN
            UPDATE SET %s
            WHEN NOT MATCHED THEN
            INSERT (%s) VALUES (%s)
            ;'''
            match_clause = ' AND '.join("%s.%s = %s.%s" % (src_alias, x, dest_alias, x) for x in match_columns)
            update_clause = ', '.join('%s = %s.%s' % (x, src_alias, x) for x in metadata.non_identity_columns if x not in match_columns) \
                + ''.join(", %s = %s" % (k, v) for k, v in additional_columns.iteritems())
            insert_columns = ', '.join(x for x in metadata.non_identity_columns) \
                + ''.join(", %s" % k for k in additional_columns.keys())
            insert_values = ', '.join('%s.%s' % (src_alias, x) for x in metadata.non_identity_columns) \
                + ''.join(", %s" % v for v in additional_columns.values())
            sql = merge_sql % (metadata.table_name, dest_alias, staging_table_name, src_alias, match_clause,update_clause, insert_columns, insert_values)
            merge_result = conn.execute(sql)
            print("Merged %d row(s) from %s into %s" % (merge_result.rowcount, table_name, staging_table_name))
            
        finally:
            if conn != None:
                conn.execute("drop table " + staging_table_name)
                conn.close()
    
    def execute(self, sql):
        with self.create_connection() as conn:
            return conn.execute(sql)
        
    @staticmethod
    def _create_staging_table_name(table_name, persist_between_connections=True):
        if persist_between_connections:
            prefix = '##'
        else:
            prefix = '#'
        return prefix + table_name + '_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")


class DatabaseConnector(object):

    @classmethod
    def create_engine(cls):
        engine = create_engine(cls.connection_string)
        return engine

class DatabaseConnectionHolder(object):
    
    def __init__(self, engine):
        self._engine = engine
    
    def __enter__(self):
        self._conn = self._engine.connect()
        return self
    
    def __exit__(self, type_, value, traceback):
        if self._conn != None:
            self._conn.close()
            
    def execute(self, sql):
        if self._conn == None:
            raise DatabaseInterfaceError("Connection not established inside DatabaseConnectionHolder")
        return self._conn.execute(sql)


class MSSQLDatabaseConnector(DatabaseConnector):
    
    connection_string = "mssql+pyodbc://localhost\\SQLEXPRESS/shipping?driver=ODBC+Driver+11+for+SQL+Server"


class TableMetadata(object):
    
    cached = {}

    def __init__(self, engine, table_name, database = None, owner = 'dbo'):
        self._engine = engine
        self.database = database
        self.owner = owner
        self.table_name = table_name
        
        self.load_columns()
        
        TableMetadata.cached[table_name] = self
        TableMetadata.cached[self.full_table_name] = self
    
    @staticmethod
    def get(engine, table_name):
        metadata = TableMetadata.cached.get(table_name)
        if metadata != None:
            return metadata
        
        elements = table_name.split('.')
        if len(elements) == 1:
            return TableMetadata(engine, elements[0])
        elif len(elements) == 2:
            return TableMetadata(engine, elements[1], owner = elements[0])
        elif len(elements) == 3:
            return TableMetadata(engine, elements[2], database = elements[0], owner = elements[1])
        else:
            return None
    
    def load_columns(self):
        sql = '''
        select TABLE_CATALOG AS 'database', TABLE_SCHEMA AS 'owner', TABLE_NAME as 'table_name', COLUMN_NAME as 'column_name', COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS 'is_identity'
        from INFORMATION_SCHEMA.COLUMNS
        where TABLE_NAME = '%s'
        '''
        
        if self.database != None:
            sql = sql + " and TABLE_CATALOG = '%s'" % self.database
        
        if self.owner != None:
            sql = sql + " and TABLE_SCHEMA = '%s'" % self.owner
                
        df = pd.read_sql_query(sql % self.table_name, self._engine, index_col=None)
        
        databases = set(df.database.tolist())
        if len(databases) > 1:
            raise DatabaseInterfaceError("Table %s exists in multiple databases (%s)" % (self.table_name, ', '.join(databases)))
        
        self.full_table_name = "%s.%s.%s" % (df.database[0], df.owner[0], df.table_name[0])
        df_identity_columns = df[df.is_identity == 1]
        self.non_identity_columns = df[df.is_identity == 0].column_name.tolist()
        if len(df_identity_columns) == 1:
            self.identity_column = str(df_identity_columns.column_name[0])
        else:
            self.identity_column = None
    

class DatabaseInterfaceError(Exception):
    pass    


class TableMetadataTests(unittest.TestCase):
    
    def test_get_identity_column(self):
        metadata = TableMetadata(MSSQLDatabaseConnector.create_engine(), 'ais_positions')
        print(metadata.full_table_name)
        print(metadata.identity_column)
        print(metadata.non_identity_columns)

class DatabaseConnectionTests(unittest.TestCase):
    
    def test_select_dataframe(self):
        di = DatabaseInterface(MSSQLDatabaseConnector())
        df = di.select_dataframe("select * from ais_positions")
    
    def test_insert_dataframe(self):
        di = DatabaseInterface(MSSQLDatabaseConnector())
        df = di.select_dataframe("select * from ais_positions")
        df = df[df.ais_position_id == 1]
        di.insert_dataframe(df, "ais_positions")

    def test_merge_dataframe(self):
        di = DatabaseInterface(MSSQLDatabaseConnector())
        df = di.select_dataframe("select * from ais_positions")
        di.merge_dataframe(df, "ais_positions" ,["imo", "model_timestamp"])

class DatabaseConnectionHolderTests(unittest.TestCase):
    
    def test_connection_holder(self):
        di = DatabaseInterface(MSSQLDatabaseConnector())
        with di.create_connection() as conn:
            pass
        