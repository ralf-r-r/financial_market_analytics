class SqlQueries:

    spot_prices_insert_old = ("""
    INSERT INTO spot_prices (symbol, year, month, day, closing_price, volume, quarter)
    SELECT stage.symbol, stage.year, stage.month, stage.day, 
    MAX(stage.closing_price) as closing_price, 
    MAX(stage.volume) as volume,
    MAX(CONCAT('Q',CAST( CAST(FLOOR((stage.month-1)/3)+1 as INT) as varchar ))) as quarter
    FROM public.staging_spot_prices as stage
    LEFT JOIN spot_prices
    ON spot_prices.symbol = stage.symbol
    AND spot_prices.year = stage.year
    AND spot_prices.day = stage.day
    WHERE spot_prices.closing_price IS NULL
    GROUP BY stage.symbol, stage.year, stage.month, stage.day;
    """)

    # the query drops duplicates from the staging table and inserts only new rows to the target table
    spot_prices_insert = ("""
    INSERT INTO spot_prices (symbol, year, month, day, closing_price, volume, quarter)
    SELECT stage.symbol, stage.year, stage.month, stage.day, 
    MAX(stage.closing_price) as closing_price, 
    MAX(stage.volume) as volume,
    MAX(CONCAT('Q',CAST( CAST(FLOOR((stage.month-1)/3)+1 as INT) as varchar ))) as quarter
    FROM
    (WITH added_row_number AS (
  		SELECT *,
    	ROW_NUMBER() OVER(
          PARTITION BY symbol,year, month, day 
          ORDER BY symbol, year, month, day DESC
          ) AS row_number
  		FROM public.staging_spot_prices
      ) SELECT * FROM added_row_number
		WHERE row_number = 1
    ) as stage
    LEFT JOIN spot_prices
    ON spot_prices.symbol = stage.symbol
    AND spot_prices.year = stage.year
    AND spot_prices.day = stage.day
    WHERE spot_prices.closing_price IS NULL
    GROUP BY stage.symbol, stage.year, stage.month, stage.day;
    """)

    fundamentals_insert_old = ("""
    INSERT INTO fundamentals (ticker, dimension, year, month, day, liabilities, debt,
    equity, assets, marketcap, netinc, ebit, shareswa, shareswadil)
    SELECT ticker, dimension,
    CAST(SUBSTRING(CAST(reportperiod as varchar), 1, 4) as INT) as year,
    CAST(SUBSTRING(CAST(reportperiod as varchar), 6, 2) as INT) as month,
    CAST(SUBSTRING(CAST(reportperiod as varchar), 9, 2) as INT) as day,
    liabilities, debt,
    equity, assets, marketcap, netinc, ebit, shareswa, shareswadil
    FROM public.staging_fundamentals;
    """)

    # the query drops duplicates from the staging table and inserts only new rows to the target table
    fundamentals_insert = ("""
    INSERT INTO fundamentals (ticker, dimension, year, month, day, liabilities, debt,
    equity, assets, marketcap, netinc, ebit, shareswa, shareswadil)
    SELECT stage.ticker, stage.dimension,
	stage.year, stage.month, stage.day,
    stage.liabilities, stage.debt,
    stage.equity, stage.assets, stage.marketcap, stage.netinc, 
    stage.ebit, stage.shareswa, stage.shareswadil
    FROM
    (WITH added_row_number AS (
  		SELECT *,
        CAST(SUBSTRING(CAST(reportperiod as varchar), 1, 4) as INT) as year,
        CAST(SUBSTRING(CAST(reportperiod as varchar), 6, 2) as INT) as month,
        CAST(SUBSTRING(CAST(reportperiod as varchar), 9, 2) as INT) as day,
    	ROW_NUMBER() OVER(
          PARTITION BY ticker, dimension, reportperiod
          ORDER BY ticker, dimension, reportperiod DESC
          ) AS row_number
  		FROM public.staging_fundamentals
      ) SELECT * FROM added_row_number
		WHERE row_number = 1
    ) as stage
    LEFT JOIN fundamentals
    ON fundamentals.ticker = stage.ticker
    AND fundamentals.dimension = stage.dimension
    AND fundamentals.year = stage.year
    AND fundamentals.month = stage.month
    AND fundamentals.day = stage.day
    WHERE fundamentals.dimension IS NULL;
    """)

    tickers_insert_old = ("""
    INSERT INTO tickers (symbol, name, country, indices)
    SELECT symbol, name, country, indices FROM public.staging_tickers;
    """)

    # the query drops duplicates from the staging table and inserts only new rows to the target table
    tickers_insert = ("""
    INSERT INTO tickers (symbol, name, country, indices)
    SELECT stage.symbol, stage.name, stage.country, stage.indices 
    FROM
    (WITH added_row_number AS (
            SELECT *,
            ROW_NUMBER() OVER(
              PARTITION BY symbol
              ORDER BY symbol DESC
              ) AS row_number
            FROM public.staging_tickers
          ) SELECT * FROM added_row_number
        WHERE row_number = 1) as stage
    LEFT JOIN tickers
    ON tickers.symbol = stage.symbol
    WHERE tickers.name is NULL;
    """)

    m3_money_supply_insert_old = ("""
    INSERT INTO m3_money_supply (value, country , year, quarter)
    SELECT value, country, 
    CAST(SUBSTRING(CAST(time as varchar), 1, 4) as INT) as year,
    CAST(SUBSTRING(CAST(time as varchar), 6, 7) as VARCHAR) as quarter
    FROM public.staging_m3_money_supply
    WHERE frequency = 'Quarterly';
    """)

    # the query drops duplicates from the staging table and inserts only new rows to the target table
    m3_money_supply_insert = ("""
    INSERT INTO m3_money_supply (value, country , year, quarter)
    SELECT stage.value, stage.country, stage.year, stage.quarter
    FROM
        (WITH added_row_number AS (
            SELECT *,
            CAST(SUBSTRING(CAST(time as varchar), 1, 4) as INT) as year,
            CAST(SUBSTRING(CAST(time as varchar), 6, 7) as VARCHAR) as quarter,
            ROW_NUMBER() OVER(
              PARTITION BY country, frequency, time
              ORDER BY country, frequency, time DESC
              ) AS row_number
            FROM public.staging_m3_money_supply
          ) SELECT * FROM added_row_number
            WHERE row_number = 1
        ) as stage
    LEFT JOIN m3_money_supply
    ON m3_money_supply.year = stage.year
    AND m3_money_supply.quarter = stage.quarter
    AND m3_money_supply.country = stage.country
    WHERE m3_money_supply.value is NULL
    AND stage.frequency = 'Quarterly';
    """)
