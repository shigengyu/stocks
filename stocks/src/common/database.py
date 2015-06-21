from sqlalchemy.engine import create_engine
import unittest
import pandas as pd
import datetime
import abc
from _pyio import __metaclass__
from common import oo

class DatabaseInterface(object):
    
    __metaclass__ = abc.ABCMeta

    def __init__(self, connector):
        self._connector = connector
        self._engine = self._connector.create_engine()
        self._engine.echo = True
        
    def create_connection(self):
        """
        Creates a database connection holder which wraps a transaction created from the SQLAlchemy engine        
        
        Returns
        -------
        conn : DatabaseConnectionHolder
            The database connection holder
        """
        conn = DatabaseConnectionHolder(self._engine)
        return conn
    
    def create_transaction(self):
        """
        Creates a database transaction holder which wraps a transaction created from the SQLAlchemy engine
        
        Returns
        -------
        trans : DatabaseTransactionHolder
            The database transaction holder
        """
        trans = DatabaseTransactionHolder(self._engine)
        return trans
    
    def select_dataframe(self, sql):
        """
        Executes a SQL query and dumps the result into a pandas DataFrame
        
        Parameters
        ----------
        sql : str
            The SQL to execute
        
        Returns
        -------
        df : DataFrame
        """
        df = pd.read_sql_query(sql, self._engine)
        return df
    
    def insert_dataframe(self, dataframe, table_name, exclude_columns = [], create_table_if_not_exist=False):
        """
        Inserts a pandas DataFrame into a database table
        
        Parameters
        ----------
        dataframe : DataFrame
            The data frame to be inserted
        
        table_name : str
            The database table to insert
            
        exclude_columns : list of str
            The data frame column names to be excluded from the insert
        
        create_table_if_not_exist : bool, default False
            Whether or not to create the database table if it does not exist
        """
        try:
            metadata = TableMetadata.get(self._engine, table_name)
        except DatabaseTableMetadataError as e:
            if not create_table_if_not_exist:
                raise e
            
        if metadata is not None and metadata.identity_column is not None:
            del dataframe[metadata.identity_column]
        for column in exclude_columns:
            del dataframe[column]
        dataframe.to_sql(metadata.table_name, self._engine, if_exists = 'append', index = False)
    
    @abc.abstractmethod
    def merge_dataframe(self, dataframe, table_name, match_columns=[], exclude_columns = [], additional_columns = {}, src_alias='src', dest_alias='dest'):
        """
        Merges a pandas DataFrame into a database table
        
        Parameters
        ----------
        dataframe : DataFrame
            The data frame to be inserted
        
        table_name : str
            The database table to merge
        
        match_columns : list of str
            Data frame column names to be used in the SQL MATCH clause
        
        exclude_columns : list of str, default empty list
            The data frame column names to be excluded from the merge
        
        additional_columns : dict of str : str, default empty dict
            Additional column names and values to be included in the merge, which is not included in the data frame. Most likely used for database side generated values
            e.g. { 'merge_timestamp', 'GETDATE()' }
        
        src_alias : str, default 'src'
            The alias of the merge source table, i.e. the temporary table which the data frame is loaded into
        
        dest_alias : str, default 'dest'
            The alias of the merge target table
        """
        pass
    
    def execute(self, sql):
        """
        Executes a SQL query
        
        Parameters
        ----------
        sql : str
            The SQL to execute
        
        Returns
        -------
        result :
            Result of the SQL execution
        """
        with self.create_connection() as conn:
            result = conn.execute(sql)
            return result
        
    @staticmethod
    def _create_staging_table_name(table_name, persist_between_connections=False):
        if persist_between_connections:
            prefix = '##'
        else:
            prefix = '#'
        return prefix + table_name + '_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")


class MSSQLDatabaseInterface(DatabaseInterface):
        
    @oo.override
    def merge_dataframe(self, dataframe, table_name, match_columns=[], exclude_columns = [], additional_columns = {}, src_alias='src', dest_alias='dest'):
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
            MERGE INTO {dest_name} {dest_alias}
            USING {src_name} {src_alias}
            ON {match_clause}
            WHEN MATCHED THEN
            UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
            INSERT ({insert_columns}) VALUES ({insert_values})
            ;'''
            sql = merge_sql.format(
                dest_name = metadata.table_name,
                dest_alias = dest_alias,
                src_name = staging_table_name,
                src_alias = src_alias,
                match_clause = ' AND '.join("%s.%s = %s.%s" % (src_alias, x, dest_alias, x) for x in match_columns),
                update_clause = ', '.join('%s = %s.%s' % (x, src_alias, x) for x in metadata.non_identity_columns if x not in match_columns and x not in exclude_columns) \
                    + ''.join(", %s = %s" % (k, v) for k, v in additional_columns.iteritems()),
                insert_columns = ', '.join(x for x in metadata.non_identity_columns if x not in exclude_columns) \
                    + ''.join(", %s" % k for k in additional_columns.keys()),
                insert_values = ', '.join('%s.%s' % (src_alias, x) for x in metadata.non_identity_columns if x not in exclude_columns) \
                    + ''.join(", %s" % v for v in additional_columns.values())
                )
            merge_result = conn.execute(sql)
            print("Merged %d row(s) from %s into %s" % (merge_result.rowcount, table_name, staging_table_name))
            
        finally:
            if conn is not None:
                conn.execute("drop table " + staging_table_name)
                conn.close()


class DatabaseConnector(object):

    __metaclass__ = abc.ABCMeta

    @classmethod
    def create_engine(cls):
        engine = create_engine(cls.connection_string)
        return engine


class MSSQLDatabaseConnector(DatabaseConnector):
    
    connection_string = "mssql+pyodbc://localhost\\SQLEXPRESS/shipping?driver=ODBC+Driver+11+for+SQL+Server"


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


class TableMetadata(object):
    
    _cached = {}

    def __init__(self, engine, table_name, database = None, owner = 'dbo'):
        """
        Initializes this TableMetadata instance
        
        Parameters
        ----------
        engine :
            The SQLAlchemy engine
        
        table_name : str
            The name of the table
        
        database : str, default None
            The name of the database
        
        owner : str, default 'dbo'
            The owner of the table
        
        """
        self._engine = engine
        self.database = database
        self.owner = owner
        self.table_name = table_name
        
        self._load_columns()
        
        TableMetadata._cached[table_name] = self
        TableMetadata._cached[self.full_table_name] = self
    
    @staticmethod
    def get(engine, table_name):
        """
        Caches and returns the metadata of a table
        
        Parameters
        ----------
        table_name : str
            The database table name, in one of the following formats:
                <database>.<owner>.<table name>
                <owner>.<table name>
                <table name>
        
        Returns
        -------
        metadata : TableMetadata
            The metadata of the table
        
        Raises
        ------
        DatabaseTableMetadataError : Error occured when retrieving database metadata
        
        """
        metadata = TableMetadata._cached.get(table_name)
        if metadata is not None:
            return metadata
        
        elements = table_name.split('.')
        if len(elements) == 1:
            metadata = TableMetadata(engine, elements[0])
        elif len(elements) == 2:
            metadata = TableMetadata(engine, elements[1], owner = elements[0])
        elif len(elements) == 3:
            metadata = TableMetadata(engine, elements[2], database = elements[0], owner = elements[1])
        
        return metadata
    
    def _load_columns(self):
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
        if len(df.index) == 0:
            raise DatabaseTableMetadataError("Failed to load metadata as table [%s] does not exist" % self.table_name)
        
        databases = set(df.database.tolist())
        if len(databases) > 1:
            raise DatabaseTableMetadataError("Table %s exists in multiple databases (%s)" % (self.table_name, ', '.join(databases)))
        
        self.full_table_name = "%s.%s.%s" % (df.database[0], df.owner[0], df.table_name[0])
        df_identity_columns = df[df.is_identity == 1]
        self.non_identity_columns = df[df.is_identity == 0].column_name.tolist()
        if len(df_identity_columns) == 1:
            self.identity_column = str(df_identity_columns.column_name[0])
        else:
            self.identity_column = None
    

class DatabaseInterfaceError(Exception):
    pass    

class DatabaseTableMetadataError(Exception):
    pass

class TableMetadataTests(unittest.TestCase):
    
    def test_get_identity_column(self):
        metadata = TableMetadata(MSSQLDatabaseConnector.create_engine(), 'ais_positions')
        assert(metadata.full_table_name == 'shipping.dbo.ais_positions')
        assert(metadata.identity_column == 'ais_position_id')
        assert(len(metadata.non_identity_columns) > 0)

class DatabaseConnectionTests(unittest.TestCase):
    
    def setUp(self):
        self.di = MSSQLDatabaseInterface(MSSQLDatabaseConnector())
    
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
        di = MSSQLDatabaseInterface(MSSQLDatabaseConnector())
        with di.create_connection() as conn:
            assert(conn is not None)


class DataTransactionHolderTests(unittest.TestCase):

    unittest_table_name = '##unittest_transaction'

    def setUp(self):
        self.di = MSSQLDatabaseInterface(MSSQLDatabaseConnector())
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