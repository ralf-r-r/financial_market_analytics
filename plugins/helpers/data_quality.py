def create_sql_checks():
    """
    creates a list with data quality checks that contain sql statements and expected results
    :return quality_checks: list[dict] a list that defines the quality checks to be run
    """
    sql_null_test = "SELECT COUNT(*) FROM {} WHERE {} is null"
    sql_nonempty_test = "SELECT COUNT(*) FROM {}"



    quality_checks = []
    # - check that primary key columns are not null
    table = "spot_prices"
    columns = ["symbol","year","month","day"]
    for k, column in enumerate(columns):
        quality_checks.append(
            dict(sql_query=sql_null_test.format(table,column),
                 expected_result={"condition": "==", "value": 0},
                 error_message="Data quality check failed. {} is null in table {}".format(column, table),
                 success_message="Data quality check successful. {} is not null in table {}".format(column, table)
                 )
        )

    table = "tickers"
    columns = ["symbol"]
    for k, column in enumerate(columns):
        quality_checks.append(
            dict(sql_query=sql_null_test.format(table,column),
                 expected_result={"condition": "==", "value": 0},
                 error_message="Data quality check failed. {} is null in table {}".format(column, table),
                 success_message="Data quality check successful. {} is not null in table {}".format(column, table)
                 )
        )

    table = "fundamentals"
    columns = ["ticker", "dimension", "year", "month"]
    for k, column in enumerate(columns):
        quality_checks.append(
            dict(sql_query=sql_null_test.format(table,column),
                 expected_result={"condition": "==", "value": 0},
                 error_message="Data quality check failed. {} is null in table {}".format(column, table),
                 success_message="Data quality check successful. {} is not null in table {}".format(column, table)
                 )
        )

    # - check that spot_prices staging table is not null
    tables = ["staging_spot_prices"]
    for table in tables:
        quality_checks.append(
            dict(sql_query=sql_nonempty_test.format(table),
                 expected_result={"condition": ">=", "value": 0},
                 error_message="Data quality check failed. {} is empty".format(table),
                 success_message="Data quality check successful. {} is not empty".format(table)
                 )
        )

    return quality_checks