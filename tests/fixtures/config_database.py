import pytest
import pyodbc
import psycopg2
from configparser import ConfigParser


@pytest.fixture(autouse=True, scope='session')
def config_database(fileName='config.ini', sqlServerSec='mssqlserver', postgreSQLSec='postgresql'):
    parser = ConfigParser()
    parser.read(fileName)

    cursorSQLServer = None
    if parser.has_section(sqlServerSec):
        sourceDBName = parser.get(sqlServerSec, "database")
        try:
            MSSQLServerConnection = pyodbc.connect(
                Driver=parser.get(sqlServerSec, "driver"),
                Server=parser.get(sqlServerSec, "server"),
                UID=parser.get(sqlServerSec, "uid"),
                PWD=parser.get(sqlServerSec, "pwd"),
                Database=parser.get(sqlServerSec, "database")
            )
            cursorSQLServer = MSSQLServerConnection.cursor()
        except (Exception, pyodbc.DatabaseError) as error:
            print(error)
    else:
        raise Exception('Section {0} not found in the {1} file'.format(sqlServerSec, fileName))

    cursorPostgreSQL = None
    if parser.has_section(postgreSQLSec):
        destDBName = parser.get(postgreSQLSec, "database")
        try:
            postgreSQLConnection = psycopg2.connect(
                host=parser.get(postgreSQLSec, "host"),
                user=parser.get(postgreSQLSec, "user"),
                password=parser.get(postgreSQLSec, "password"),
                database=parser.get(postgreSQLSec, "database")
            )
            cursorPostgreSQL = postgreSQLConnection.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    else:
        raise Exception('Section {0} not found in the {1} file'.format(postgreSQLSec, fileName))

    cursorsAndDBs = [(cursorSQLServer, sourceDBName, parser.get(sqlServerSec, "ignorableSchemas"), parser.get(sqlServerSec, "ignorableTables"), parser.get(sqlServerSec, "ignorableSps"), parser.get(sqlServerSec, "ignorableFunctions")),
                     (cursorPostgreSQL, destDBName, parser.get(postgreSQLSec, "ignorableSchemas"), parser.get(postgreSQLSec, "ignorableTables"), parser.get(postgreSQLSec, "ignorableSps"), parser.get(postgreSQLSec, "ignorableFunctions"))]

    yield cursorsAndDBs
    cursorSQLServer.close()
    cursorPostgreSQL.close()
    MSSQLServerConnection.close()
    postgreSQLConnection.close()