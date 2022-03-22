-- Create staging tables

CREATE TABLE public.staging_tickers (
	symbol varchar(256) NOT NULL,
	name varchar(256),
	country varchar(256),
	indices varchar(256)
);

CREATE TABLE public.staging_m3_money_supply (
	value float,
	country varchar(256),
	frequency varchar(256),
	time varchar(256)
);

CREATE TABLE public.staging_spot_prices (
	symbol varchar(256),
	year smallint,
	month smallint,
	day smallint,
	closing_price float,
	volume float
);

CREATE TABLE public.staging_fundamentals (
    ticker varchar(256),
    dimension varchar(256),
    reportperiod varchar(256),
    liabilities float,
    debt float,
    equity float,
    assets float,
    marketcap float,
    netinc float,
    ebit float,
    shareswa float,
    shareswadil float
 );

 -- create fact and dimension tables
 CREATE TABLE public.spot_prices (
	symbol varchar(256),
	year smallint,
	month smallint,
	day smallint,
	closing_price float,
	volume float,
    quarter varchar(256),
	CONSTRAINT spot_prices_pkey PRIMARY KEY (symbol,year,month,day)
);

 CREATE TABLE public.tickers (
	symbol varchar(256) NOT NULL,
	name varchar(256),
	country varchar(256),
	indices varchar(256),
	CONSTRAINT tickers_pkey PRIMARY KEY (symbol)
);

CREATE TABLE public.fundamentals (
    ticker varchar(256),
    dimension varchar(256),
	year smallint,
	month smallint,
	day smallint,
    liabilities float,
    debt float,
    equity float,
    assets float,
    marketcap float,
    netinc float,
    ebit float,
    shareswa float,
    shareswadil float,
    CONSTRAINT fundamentals_pkey PRIMARY KEY (ticker, dimension,year,month)
 );

 CREATE TABLE public.m3_money_supply (
	value float,
	country varchar(256),
    year int,
    quarter varchar(256),
    CONSTRAINT m3_money_supply_pkey PRIMARY KEY (country,year,quarter)
 );