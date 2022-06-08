import pytest


@pytest.mark.usefixtures("config_database")
def test_table_row_counts(config_database):
    config_database[0][0].execute(
        """ SELECT table_name, table_schema FROM information_schema.tables
            WHERE table_catalog = '{}' AND table_type = 'BASE TABLE' AND table_schema NOT IN {} AND table_name NOT IN {} 
            ORDER BY table_name;
        """.format(config_database[0][1], config_database[0][2], config_database[0][3]))
    tablesSQLServer = dict(config_database[0][0].fetchall())
    config_database[1][0].execute(
        """ SELECT table_name, table_schema FROM information_schema.tables
            WHERE table_type ='BASE TABLE' AND table_catalog = '{}' ORDER BY table_name;
        """.format(config_database[1][1]))
    tablesPostgreSQL = dict(config_database[1][0].fetchall())

    unmatchedRowTables = []
    for tableName in tablesSQLServer:
        if tableName.lower() in tablesPostgreSQL:
            config_database[0][0].execute(
                """ SELECT COUNT(*) FROM {}.{};""".format(tablesSQLServer[tableName], tableName))
            tableRowCountMSSQL = config_database[0][0].fetchall()
            config_database[1][0].execute(
                """ SELECT COUNT(*) FROM {}.{};""".format(tablesPostgreSQL[tableName.lower()], tableName.lower()))
            tableRowCountPostgreSQL = config_database[1][0].fetchall()
            if tableRowCountMSSQL[0][0] != tableRowCountPostgreSQL[0][0]:
                unmatchedRowTables.append(
                    (tablesSQLServer[tableName], tableName, tableRowCountMSSQL[0][0], tableRowCountPostgreSQL[0][0]))

    for row in unmatchedRowTables:
        print(row)

    assert len(unmatchedRowTables) == 0