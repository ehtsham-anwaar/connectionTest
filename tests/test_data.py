import pytest


@pytest.mark.usefixtures("config_database")
def test_table_rows(config_database):
    config_database[0][0].execute(
        """ SELECT T.TABLE_SCHEMA, T.TABLE_NAME, C.COLUMN_NAME FROM  INFORMATION_SCHEMA.TABLE_CONSTRAINTS T  
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME
            WHERE T.CONSTRAINT_TYPE = 'PRIMARY KEY' AND T.TABLE_NAME NOT IN {} AND T.TABLE_NAME IN (
	            SELECT T.TABLE_NAME FROM  INFORMATION_SCHEMA.TABLE_CONSTRAINTS T  
	            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME
	            WHERE T.CONSTRAINT_TYPE = 'PRIMARY KEY'
	            GROUP BY T.TABLE_NAME HAVING COUNT(T.TABLE_NAME) = 1
            )
            ORDER BY T.TABLE_NAME;
        """.format(config_database[0][3]))
    pkColumnsSQLServer = config_database[0][0].fetchall()

    mismacthedRowTables = []
    for table in pkColumnsSQLServer:
        config_database[0][0].execute(
            """ SELECT * FROM {}.{} ORDER BY CAST({} AS VARCHAR(MAX));
            """.format(table[0], table[1], table[2]))
        rowsSQLServer = config_database[0][0].fetchall()
        config_database[1][0].execute(
            """ SELECT * FROM {}.{} ORDER BY CAST({} AS TEXT);
            """.format(table[0].lower(), table[1].lower(), table[2].lower()))
        rowsPostgreSQL = config_database[1][0].fetchall()

        x = 0
        mismatchedRowPks = []
        mismatchedRowsCount = 0
        while x<len(rowsSQLServer):
            if list(rowsSQLServer[x]) != list(rowsPostgreSQL[x]):
                mismatchedRowPks.append(rowsSQLServer[x][0])
                mismatchedRowsCount = mismatchedRowsCount + 1
            x = x + 1

        if mismatchedRowsCount != 0:
            mismacthedRowTables.append((table[0], table[1], mismatchedRowsCount, table[2]))

    for row in mismacthedRowTables:
        print(row)

    assert len(mismacthedRowTables) == 0