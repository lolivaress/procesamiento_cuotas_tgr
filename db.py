import pyodbc
from sqlalchemy import MetaData, create_engine


#sqlalchemy class connection
class SQLAlchemyConnection:

    def __init__(self, server, database,fast=True):
        self.server = server
        self.database = database
        self.connection = None
        self.engine = None
        self.metadata = None
        self.fast = fast
   
    def connect(self):
        self.connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server +
            ';DATABASE=' + self.database + ';Trusted_Connection=yes')
        self.engine = create_engine('mssql+pyodbc:///' + self.database +
                                    '?driver=ODBC+Driver+17+for+SQL+Server',fast_executemany=self.fast)
        self.metadata = MetaData(self.engine)

    def execute(self, query):
        return self.engine.execute(query)

    def close(self):
        self.connection.close()
        self.engine.dispose()
        #self.metadata.dispose()
        self.connection = None
        self.engine = None
        self.metadata = None