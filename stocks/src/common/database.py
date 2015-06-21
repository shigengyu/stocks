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
    
    def create_transaction(self):
        return DatabaseTransactionHolder(self._engine)
    
    def select(self):
        pass
    
    def select_dataframe(self, sql):
        return pd.read_sql_query(sql, self._engine)
    
    def insert_dataframe(self, dataframe, table_name):
        metadata = TableMetadata.get(self._engine, table_name)
        if metadata.identity_column is not None:
            del dataframe[metadata.identity_column]
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
            staging_table_name = DatabaseInterface._create_staging_table_name(table_name, persist_between_connections=True)
            
            if metadata.identity_column is not None:
                del dataframe[metadata.identity_column]
            for column in exclude_columns:
                del dataframe[column]
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
            update_clause = ', '.join('%s = %s.%s' % (x, src_alias, x) for x in metadata.non_identity_columns if x not in match_columns and x not in exclude_columns) \
                + ''.join(", %s = %s" % (k, v) for k, v in additional_columns.iteritems())
            insert_columns = ', '.join(x for x in metadata.non_identity_columns if x not in exclude_columns) \
                + ''.join(", %s" % k for k in additional_columns.keys())
            insert_values = ', '.join('%s.%s' % (src_alias, x) for x in metadata.non_identity_columns if x not in exclude_columns) \
                + ''.join(", %s" % v for v in additional_columns.values())
            sql = merge_sql % (metadata.table_name, dest_alias, staging_table_name, src_alias, match_clause,update_clause, insert_columns, insert_values)
            merge_result = conn.execute(sql)
            print("Merged %d row(s) from %s into %s" % (merge_result.rowcount, table_name, staging_table_name))
            
        finally:
            if conn is not None:
                conn.execute("drop table " + staging_table_name)
                conn.close()
    
    def execute(self, sql):
        with self.create_connection() as conn:
            return conn.execute(sql)
        
    @staticmethod
    def _create_staging_table_name(table_name, persist_between_connections=False):
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
        return self.connect()
    
    def __exit__(self, type_, value, traceback):
        self.close()

    def connect(self):
        self._conn = self._engine.connect()
        print("Connection opened...")
        return self
        
    def close(self):
        if self._conn is not None:
            self._conn.close()
            print("Connection closed")

    def execute(self, sql):
        if self._conn == None:
            raise DatabaseInterfaceError("Connection not established inside DatabaseConnectionHolder")
        return self._conn.execute(sql)


class DatabaseTransactionHolder(DatabaseConnectionHolder):
    
    def __init__(self, engine):
        super(DatabaseTransactionHolder, self).__init__(engine)
        self.is_committed = False
        self.is_rolled_back = False
    
    def __enter__(self):
        super(DatabaseTransactionHolder, self).__enter__()
        self._trans = self._conn.begin()
        return self
    
    def __exit__(self, type_, value, traceback):
        if not self.is_committed and not self.is_rolled_back:
            self._trans.rollback()
            
        super(DatabaseTransactionHolder, self).__exit__(type_, value, traceback)

    def commit(self):
        if self.is_committed:
            raise DatabaseInterfaceError("Transaction as already committed")
        if self.is_rolled_back:
            raise DatabaseInterfaceError("Cannot commit transaction as already rolled back")
        self._trans.commit()
        self.is_committed = True
        
    def rollback(self):
        if self.is_committed:
            raise DatabaseInterfaceError("Cannot rollback transaction as already committed")
        if not self.is_rolled_back:
            self._trans.rollback()
            self.is_rolled_back = True


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
        if metadata is not None:
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
        
        if self.database is not None:
            sql = sql + " and TABLE_CATALOG = '%s'" % self.database
        
        if self.owner is not None:
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
    
    def setUp(self):
        self.di = DatabaseInterface(MSSQLDatabaseConnector())
    
    def test_select_dataframe(self):
        df = self.di.select_dataframe("select * from ais_positions")
        assert(df is not None)
    
    def test_insert_dataframe(self):
        df = self.di.select_dataframe("select * from ais_positions")
        df = df[df.ais_position_id == 1]
        self.di.insert_dataframe(df, "ais_positions")

    def test_merge_dataframe(self):
        df = self.di.select_dataframe("select * from ais_positions")
        self.di.merge_dataframe(df, "ais_positions" ,["imo", "model_timestamp"], exclude_columns=['report_time'], additional_columns={'report_time': 'GETDATE()'})


class DatabaseConnectionHolderTests(unittest.TestCase):
    
    def test_connection_holder(self):
        di = DatabaseInterface(MSSQLDatabaseConnector())
        with di.create_connection() as conn:
            assert(conn is not None)

class DataTransactionHolderTests(unittest.TestCase):

    unittest_table_name = '##unittest_transaction'

    def setUp(self):
        self.di = DatabaseInterface(MSSQLDatabaseConnector())
        self._conn = self.di.create_connection().connect()
        self._conn.execute("create table %s ( number int )" % DataTransactionHolderTests.unittest_table_name)
        self._conn.execute("insert into %s values (0)" % DataTransactionHolderTests.unittest_table_name)

    def test_transaction_holder_commit(self):
        with self.di.create_transaction() as trans:
            trans.execute("update %s set number = number + 1"  % DataTransactionHolderTests.unittest_table_name)
            trans.commit()
        
        number = self._conn.execute("select number from %s" % DataTransactionHolderTests.unittest_table_name).fetchone()[0]
        assert(number == 1)
    
    def test_transaction_holder_no_auto_rollback(self):
        with self.di.create_transaction() as trans:
            trans.execute("update %s set number = number + 1" % DataTransactionHolderTests.unittest_table_name)
        
        number = self._conn.execute("select number from %s" % DataTransactionHolderTests.unittest_table_name).fetchone()[0]
        assert(number == 0)
    
    def test_transaction_holder_no_manual_rollback(self):
        with self.di.create_transaction() as trans:
            trans.execute("update %s set number = number + 1" % DataTransactionHolderTests.unittest_table_name)
            trans.rollback()
        
        number = self._conn.execute("select number from %s" % DataTransactionHolderTests.unittest_table_name).fetchone()[0]
        assert(number == 0)

    def tearDown(self):
        self._conn.execute("drop table %s" % DataTransactionHolderTests.unittest_table_name)
        self._conn.close()