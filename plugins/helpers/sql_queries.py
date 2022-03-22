class SqlQueries:
    spot_prices_insert = ("""
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

    fundamentals_insert = ("""
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

    tickers_insert = ("""
    INSERT INTO tickers (symbol, name, country, indices)
    SELECT symbol, name, country, indices FROM public.staging_tickers;
    """)

    m3_money_supply_insert = ("""
    INSERT INTO m3_money_supply (value, country , year, quarter)
    SELECT value, country, 
    CAST(SUBSTRING(CAST(time as varchar), 1, 4) as INT) as year,
    CAST(SUBSTRING(CAST(time as varchar), 6, 7) as VARCHAR) as quarter
    FROM public.staging_m3_money_supply
    WHERE frequency = 'Quarterly';
    """)
