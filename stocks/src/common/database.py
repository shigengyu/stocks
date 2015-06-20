from sqlalchemy.engine import create_engine
import unittest
import pandas as pd

class DatabaseInterface(object):

    def __init__(self, connector):
        self.connector = connector
        self.engine = self.connector.create_engine()
        self.engine.echo = True
    
    def select(self):
        pass
    
    def select_dataframe(self, sql):
        return pd.read_sql_query(sql, self.engine)
    
    def insert_dataframe(self, dataframe, table_name):
        metadata = TableMetadata.get(self.engine, table_name)
        if metadata.identity_column != None:
            dataframe.drop(metadata.identity_column, axis=1, inplace=True)
        dataframe.to_sql(metadata.table_name, self.engine, if_exists = 'append', index = False)
    
    '''
    Merge data frame into table. staging_table_name must be provided as data frame to_sql does not yet support reusing connection
    '''
    def merge_dataframe(self, dataframe, table_name, match_columns, staging_table_name=None):
        metadata = TableMetadata.get(self.engine, table_name)
        try:
            conn = self.engine.connect()
            if staging_table_name == None:
                staging_table_name = '#' + table_name
                conn.execute("select top 0 * into %s from %s" % (staging_table_name, metadata.full_table_name))
            else:
                conn.execute("delete from %s" % staging_table_name)
            
            if metadata.identity_column != None:
                dataframe.drop(metadata.identity_column, axis=1, inplace=True)
            dataframe.drop_duplicates(subset=match_columns, inplace=True)
            dataframe.to_sql(staging_table_name, conn.engine, if_exists = 'append', index = False)
            
            merge_sql = '''
            MERGE INTO %s dest
            USING %s src
            ON %s
            WHEN MATCHED THEN
            UPDATE SET %s
            WHEN NOT MATCHED THEN
            INSERT (%s) VALUES (%s)
            ;'''
            match_clause = ' AND '.join("dest.%s = src.%s" % (x, x) for x in match_columns)
            update_clause = ', '.join('%s = src.%s' % (x, x) for x in metadata.non_identity_columns if x not in match_columns)
            insert_columns = ', '.join(x for x in metadata.non_identity_columns)
            insert_values = ', '.join('src.%s' % x for x in metadata.non_identity_columns)
            sql = merge_sql % (metadata.table_name, staging_table_name, match_clause,update_clause, insert_columns, insert_values)
            result = conn.execute(sql)
            print(result.rowcount)
        finally:
            if conn != None:
                conn.execute("delete from %s" % staging_table_name)
                conn.close()
    
    def execute(self, sql):
        try:
            conn = self.engine.connect()
            return conn.execute(sql)
        finally:
            if conn != None:
                conn.close()


class DatabaseConnector(object):

    @classmethod
    def create_engine(cls):
        engine = create_engine(cls.connection_string)
        return engine

class MSSQLDatabaseConnector(DatabaseConnector):
    
    connection_string = "mssql+pyodbc://localhost\\SQLEXPRESS/shipping?driver=ODBC+Driver+11+for+SQL+Server"


class TableMetadata(object):
    
    cached = {}

    def __init__(self, engine, table_name, database = None, owner = 'dbo'):
        self.engine = engine
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
                
        df = pd.read_sql_query(sql % self.table_name, self.engine, index_col=None)
        
        self.full_table_name = "%s.%s.%s" % (df.database[0], df.owner[0], df.table_name[0])
        df_identity_columns = df[df.is_identity == 1]
        self.non_identity_columns = df[df.is_identity == 0].column_name.tolist()
        if len(df_identity_columns) == 1:
            self.identity_column = str(df_identity_columns.column_name[0])
        else:
            self.identity_column = None
    

class TestTableMetadata(unittest.TestCase):
    
    def test_get_identity_column(self):
        metadata = TableMetadata(MSSQLDatabaseConnector.create_engine(), 'ais_positions')
        print(metadata.full_table_name)
        print(metadata.identity_column)
        print(metadata.non_identity_columns)

class DatabaseConnectionTest(unittest.TestCase):
    
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
        di.merge_dataframe(df, "ais_positions" ,["imo", "model_timestamp"], staging_table_name="ais_positions_staging")
