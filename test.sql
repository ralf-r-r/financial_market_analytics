INSERT INTO m3_money_supply (value, country , year, quarter)
SELECT stage.value, stage.country, stage.year, stage.quarter
FROM
    (WITH added_row_number AS (
  		SELECT *,
		CAST(SUBSTRING(CAST(time as varchar), 1, 4) as INT) as year,
		CAST(SUBSTRING(CAST(time as varchar), 6, 7) as VARCHAR) as quarter,
    	ROW_NUMBER() OVER(
          PARTITION BY time, frequency, country+
          ORDER BY time, frequency, country DESC
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