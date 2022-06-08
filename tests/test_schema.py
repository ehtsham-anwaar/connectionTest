import pytest


@pytest.mark.usefixtures("config_database")
def test_table_names(config_database):
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

    unmatchedTableNames = []
    for tableName in tablesSQLServer:
        if tableName.lower() in tablesPostgreSQL:
            pass
        else:
            unmatchedTableNames.append(
                (tablesSQLServer[tableName], tableName))

    for row in unmatchedTableNames:
        print(row)

    assert (len(unmatchedTableNames) == 0)


@pytest.mark.usefixtures("config_database")
def test_view_names(config_database):
    config_database[0][0].execute(
        """ SELECT table_name, table_schema FROM information_schema.tables
            WHERE table_catalog = '{}' AND table_type = 'VIEW' AND table_name NOT IN {} ORDER BY table_name;
        """.format(config_database[0][1], config_database[0][3]))
    viewsSQLServer = dict(config_database[0][0].fetchall())
    config_database[1][0].execute(
        """ SELECT table_name, table_schema FROM information_schema.tables
            WHERE table_type ='VIEW' AND table_catalog = '{}' ORDER BY table_name;;
        """.format(config_database[1][1]))
    viewsPostgreSQL = dict(config_database[1][0].fetchall())

    unmatchedViewNames = []
    for viewName in viewsSQLServer:
        if viewName.lower() in viewsPostgreSQL:
            pass
        else:
            unmatchedViewNames.append(
                (viewsSQLServer[viewName], viewName))

    for row in unmatchedViewNames:
        print(row)

    assert (len(unmatchedViewNames) == 0)


@pytest.mark.usefixtures("config_database")
def test_stored_procedure_names(config_database):
    config_database[0][0].execute(
        """ SELECT name, Schema_name(schema_id) FROM sys.objects 
            WHERE TYPE = 'P' AND name NOT IN {} ORDER BY name;
        """.format(config_database[0][4]))
    spsSQLServer = dict(config_database[0][0].fetchall())
    config_database[1][0].execute(
        """ SELECT p.proname, n.nspname FROM pg_proc p join pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind = 'p' ORDER BY p.proname;
        """)
    spsPostgreSQL = dict(config_database[1][0].fetchall())

    unmatchedspNames = []
    for spName in spsSQLServer:
        if spName.lower() in spsPostgreSQL:
            pass
        else:
            unmatchedspNames.append(
                (spsSQLServer[spName], spName))

    for row in unmatchedspNames:
        print(row)

    assert (len(unmatchedspNames) == 0)


@pytest.mark.usefixtures("config_database")
def test_function_names(config_database):
    config_database[0][0].execute(
        """ SELECT name, Schema_name(schema_id) FROM sys.objects
            WHERE TYPE IN ('FN', 'TF', 'IF', 'FT', 'FS') AND name NOT IN {} ORDER BY name;
        """.format(config_database[0][5]))
    functionsSQLServer = dict(config_database[0][0].fetchall())
    config_database[1][0].execute(
        """ SELECT p.proname, n.nspname FROM pg_proc p join pg_namespace n ON p.pronamespace = n.oid
            WHERE p.prokind = 'f' ORDER BY p.proname;
        """)
    functionsPostgreSQL = dict(config_database[1][0].fetchall())

    unmatchedFunctionNames = []
    for functionName in functionsSQLServer:
        if functionName.lower() in functionsPostgreSQL:
            pass
        else:
            unmatchedFunctionNames.append(
                (functionsSQLServer[functionName], functionName))

    for row in unmatchedFunctionNames:
        print(row)

    assert (len(unmatchedFunctionNames) == 0)


@pytest.mark.usefixtures("config_database")
def test_column_counts(config_database):
    config_database[0][0].execute(
        """ SELECT TABLE_NAME, COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_CATALOG = '{}' AND TABLE_SCHEMA NOT IN {} AND TABLE_NAME NOT IN {}
            GROUP BY TABLE_NAME ORDER BY TABLE_NAME;
        """.format(config_database[0][1], config_database[0][2], config_database[0][3]))
    columnCountsSQLServer = dict(config_database[0][0].fetchall())
    config_database[1][0].execute(
        """ SELECT table_name, COUNT(*) FROM information_schema.columns
            WHERE table_catalog = '{}' GROUP BY table_name ORDER BY table_name;
        """.format(config_database[1][1]))
    columnCountsPostgreSQL = dict(config_database[1][0].fetchall())

    unmatchedColumnCounts = []
    for tableName in columnCountsSQLServer:
        if tableName.lower() in columnCountsPostgreSQL:
            if columnCountsSQLServer[tableName] != columnCountsPostgreSQL[tableName.lower()]:
                unmatchedColumnCounts.append((tableName, columnCountsSQLServer, columnCountsPostgreSQL))

    for row in unmatchedColumnCounts:
        print(row)

    assert (len(unmatchedColumnCounts) == 0)


@pytest.mark.usefixtures("config_database")
def test_column_names(config_database):
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

    unmatchedColumnNames = []
    for tableName in tablesSQLServer:
        if tableName.lower() in tablesPostgreSQL:
            config_database[0][0].execute(
                """ SELECT COLUMN_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_CATALOG = '{}' AND TABLE_NAME = '{}' ORDER BY COLUMN_NAME;
                """.format(config_database[0][1], tableName))
            columnsSQLServer = dict(config_database[0][0].fetchall())
            config_database[1][0].execute(
                """ SELECT column_name, table_name FROM information_schema.columns 
                    WHERE table_catalog = '{}' AND table_name = '{}' ORDER BY column_name;
                """.format(config_database[1][1], tableName.lower()))
            columnsPostgreSQL = dict(config_database[1][0].fetchall())
            for columnName in columnsSQLServer:
                if columnName.lower() in columnsPostgreSQL:
                    pass
                else:
                    unmatchedColumnNames.append((tableName, columnName))

    for row in unmatchedColumnNames:
        print(row)

    assert (len(unmatchedColumnNames) == 0)


@pytest.mark.usefixtures("config_database")
def test_column_ordinal_positions(config_database):
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

    unmatchedColumnOrdinalPositions = []
    for tableName in tablesSQLServer:
        if tableName.lower() in tablesPostgreSQL:
            config_database[0][0].execute(
                """ SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = '{}' AND TABLE_NAME  = '{}' ORDER BY ORDINAL_POSITION;
                """.format(config_database[0][1], tableName))
            ordinalPositionsSQLServer = dict(config_database[0][0].fetchall())
            config_database[1][0].execute(
                """ SELECT column_name, ordinal_position FROM information_schema.columns 
                    WHERE table_catalog = '{}' AND table_name = '{}' ORDER BY ordinal_position;
                """.format(config_database[1][1], tableName.lower()))
            ordinalPositionsPostgreSQL = dict(config_database[1][0].fetchall())
            for columnName in ordinalPositionsSQLServer:
                if columnName.lower() in ordinalPositionsPostgreSQL:
                    if ordinalPositionsSQLServer[columnName] != ordinalPositionsPostgreSQL[columnName.lower()]:
                        unmatchedColumnOrdinalPositions.append((tableName, columnName, ordinalPositionsSQLServer[columnName], ordinalPositionsPostgreSQL[columnName.lower()]))

    for row in unmatchedColumnOrdinalPositions:
        print(row)

    assert (len(unmatchedColumnOrdinalPositions) == 0)


@pytest.mark.usefixtures("config_database")
def test_column_data_types(config_database):
    dataTypesMapping = {
        'bigint': ('bigint'), 'bit': ('boolean'),
        'datetime': ('timestamp without time zone'), 'decimal': ('numeric'),
        'int': ('integer'), 'nchar': ('character'),
        'nvarchar': ('character varying', 'text'), 'uniqueidentifier': ('uuid'),
        'varbinary': ('varbinary')
    }

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

    unmatchedColumnDataTypes = []
    for tableName in tablesSQLServer:
        if tableName.lower() in tablesPostgreSQL:
            config_database[0][0].execute(
                """ SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = '{}' AND TABLE_NAME  = '{}' ORDER BY DATA_TYPE;
                """.format(config_database[0][1], tableName))
            dataTypesSQLServer = dict(config_database[0][0].fetchall())
            config_database[1][0].execute(
                """ SELECT column_name, data_type FROM information_schema.columns 
                    WHERE table_catalog = '{}' AND table_name = '{}' ORDER BY data_type;
                """.format(config_database[1][1], tableName.lower()))
            dataTypesPostgreSQL = dict(config_database[1][0].fetchall())
            for columnName in dataTypesSQLServer:
                if columnName.lower() in dataTypesPostgreSQL:
                    if dataTypesPostgreSQL[columnName.lower()] in dataTypesMapping[dataTypesSQLServer[columnName]]:
                        pass
                    else:
                        unmatchedColumnDataTypes.append((tableName, columnName, dataTypesSQLServer[columnName], dataTypesPostgreSQL[columnName.lower()]))

    for row in unmatchedColumnDataTypes:
        print(row)

    assert (len(unmatchedColumnDataTypes) == 0)


@pytest.mark.usefixtures("config_database")
def test_column_characters_maximum_length(config_database):
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

    unmatchedColumnOrdinalPositions = []
    for tableName in tablesSQLServer:
        if tableName.lower() in tablesPostgreSQL:
            config_database[0][0].execute(
                """ SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = '{}' AND TABLE_NAME  = '{}' AND CHARACTER_MAXIMUM_LENGTH NOT IN (-1)
                    ORDER BY CHARACTER_MAXIMUM_LENGTH;
                """.format(config_database[0][1], tableName))
            charMaxLengthSQLServer = dict(config_database[0][0].fetchall())
            config_database[1][0].execute(
                """ SELECT column_name, character_maximum_length FROM information_schema.columns 
                    WHERE table_catalog = '{}' AND table_name = '{}' AND character_maximum_length NOT IN (-1)
                    ORDER BY character_maximum_length;
                """.format(config_database[1][1], tableName.lower()))
            charMaxLengthPostgreSQL = dict(config_database[1][0].fetchall())
            for columnName in charMaxLengthSQLServer:
                if columnName.lower() in charMaxLengthPostgreSQL:
                    if charMaxLengthSQLServer[columnName] > charMaxLengthPostgreSQL[columnName.lower()]:
                        unmatchedColumnOrdinalPositions.append((tableName, columnName, charMaxLengthSQLServer[columnName], charMaxLengthPostgreSQL[columnName.lower()]))

    for row in unmatchedColumnOrdinalPositions:
        print(row)

    assert (len(unmatchedColumnOrdinalPositions) == 0)