import pytest


@pytest.mark.usefixtures("config_database")
def test_row_comparison(config_database):
    config_database[0][0].execute(
        """ SELECT T.TABLE_SCHEMA, T.TABLE_NAME, C.COLUMN_NAME FROM  INFORMATION_SCHEMA.TABLE_CONSTRAINTS T  
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME
            WHERE T.CONSTRAINT_TYPE = 'PRIMARY KEY' AND T.TABLE_NAME NOT IN {}
            ORDER BY T.TABLE_NAME;
        """.format(config_database[0][3]))
    pkColumnsSQLServer = config_database[0][0].fetchall()

    mismacthedRowTables = []
    for table in pkColumnsSQLServer:
        config_database[0][0].execute(
            """ SELECT TOP(2000000) * FROM {}.{} ORDER BY {};
            """.format(table[0], table[1], table[2]))
        rowsSQLServer = config_database[0][0].fetchall()
        config_database[1][0].execute(
            """ SELECT * FROM {}.{} ORDER BY {} LIMIT 2000000;
            """.format(table[0].lower(), table[1].lower(), table[2].lower()))
        rowsPostgreSQL = config_database[1][0].fetchall()

        x = 0
        mismatchedRowPks = []
        mimatchedRowsCount = 0
        while x<len(rowsSQLServer):
            if list(rowsSQLServer[x]) != list(rowsPostgreSQL[x]):
                mismatchedRowPks.append(rowsSQLServer[x][0])
                mimatchedRowsCount = mimatchedRowsCount + 1
            x = x + 1

        if mimatchedRowsCount != 0:
            mismacthedRowTables.append((table[0], table[1], mimatchedRowsCount, table[2], mismatchedRowPks))

    for row in mismacthedRowTables:
        print(row)

    assert len(mismacthedRowTables) == 0
