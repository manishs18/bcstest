create DATABASE bcs

CREATE TABLE nifty_tick_data (
    "Timestamp" TIMESTAMP,
    "Open" DECIMAL(10, 2),
    "High" DECIMAL(10, 2),
    "Low" DECIMAL(10, 2),
    "Close" DECIMAL(10, 2),
    "Volume" INTEGER,
    "Bid" DECIMAL(10, 2),
    "Ask" DECIMAL(10, 2)
);


COPY nifty_tick_data ("Timestamp", "Open", "High", "Low", "Close", "Volume", "Bid", "Ask")
FROM 'C:\Temp\nifty_tick_data.csv'
DELIMITER ','
CSV HEADER;


SELECT * from nifty_tick_data


CREATE TABLE option_chain_tick_data (
    "Timestamp" TIMESTAMP,
    "Strike Price" INTEGER,
    "Option Type" VARCHAR(30),
    "Bid Price" DECIMAL(10, 2),
    "Ask Price" DECIMAL(10, 2),
    "Volume" INTEGER,
    "Open Interest" INTEGER
);



COPY option_chain_tick_data ("Timestamp", "Strike Price", "Option Type", "Bid Price", "Ask Price", "Volume", "Open Interest")
FROM 'C:\Temp\option_chain_tick_data.csv'
DELIMITER ','
CSV HEADER;


SELECT * from option_chain_tick_data

-- task1
-- Basic stats for Nifty Tick Data
SELECT
    AVG("Open") AS mean_open,
    AVG("Close") AS mean_close,
    AVG("Volume") AS mean_volume,
    MIN("Open") AS min_open,
    MIN("Close") AS min_close,
    MIN("Volume") AS min_volume,
    MAX("Open") AS max_open,
    MAX("Close") AS max_close,
    MAX("Volume") AS max_volume
FROM nifty_tick_data;

-- Basic stats for Option Chain Data
SELECT
    AVG("Strike Price") AS mean_strike_price,
    AVG("Volume") AS mean_volume,
    AVG("Open Interest") AS mean_open_interest,
    MIN("Strike Price") AS min_strike_price,
    MIN("Volume") AS min_volume,
    MIN("Open Interest") AS min_open_interest,
    MAX("Strike Price") AS max_strike_price,
    MAX("Volume") AS max_volume,
    MAX("Open Interest") AS max_open_interest
FROM option_chain_tick_data;


-- task2
-- Total volume for each option type
SELECT 
    "Option Type", 
    SUM("Volume") AS total_volume
FROM option_chain_tick_data
GROUP BY "Option Type";


--  task3
-- Identify rows with missing values
SELECT *
FROM option_chain_tick_data
WHERE "Strike Price" IS NULL OR "Volume" IS NULL OR "Open Interest" IS NULL;

-- Delete rows with missing values
DELETE FROM option_chain_tick_data
WHERE "Strike Price" IS NULL OR "Volume" IS NULL OR "Open Interest" IS NULL;


-- task4
-- Convert Timestamp to a standard format
UPDATE option_chain_tick_data
SET "Timestamp" = "Timestamp"::TIMESTAMP
WHERE "Timestamp" IS NOT NULL





-- task5
-- Filter options with Strike Price > 15000 and Volume > 1000
SELECT *
FROM option_chain_tick_data
WHERE "Strike Price" > 15000 AND "Volume" > 1000
LIMIT 5;



-- task6
-- Get LTP for a specific option, e.g., NIFTY 15000 CE
SELECT "Timestamp", "Bid Price" AS LTP
FROM option_chain_tick_data
WHERE "Option Type" = 'Call' AND "Strike Price" = 15000
ORDER BY "Timestamp";




-- task7
-- Get open interest for Calls and Puts over time for the last expiry
SELECT "Timestamp", "Option Type", "Open Interest"
FROM option_chain_tick_data
WHERE "Timestamp"= (SELECT MAX("Timestamp") FROM option_chain_tick_data)
ORDER BY "Timestamp";



-- task8
-- Identify opportunities for buying Call options
WITH potential_trades AS (
    SELECT *,
           LAG("Bid Price", 1) OVER (PARTITION BY "Strike Price" ORDER BY "Timestamp") AS previous_bid_price,
           LAG("Timestamp", 1) OVER (PARTITION BY "Strike Price" ORDER BY "Timestamp") AS previous_timestamp
    FROM option_chain_tick_data
    WHERE "Option Type" = 'Call'
)
SELECT *
FROM potential_trades
WHERE "Bid Price" >= previous_bid_price * 1.05
AND ("Timestamp" - previous_timestamp) <= INTERVAL '10 minutes';



-- task9
-- Calculate profit/loss based on previous LTP
WITH call_options AS (
    SELECT 
        "Timestamp", 
        "Bid Price", 
        LAG("Bid Price") OVER (ORDER BY "Timestamp") AS previous_bid_price
    FROM option_chain_tick_data
    WHERE "Option Type" = 'Call'
    AND "Timestamp" BETWEEN '2024-09-01' AND '2024-09-07'  -- Adjust the dates as needed
),
potential_trades AS (
    SELECT 
        *,
        CASE 
            WHEN "Bid Price" >= previous_bid_price * 1.05 THEN 'Sell' 
            ELSE NULL 
        END AS trade_signal
    FROM call_options
),
trades AS (
    SELECT 
        *,
        SUM(CASE WHEN trade_signal = 'Sell' THEN 1 ELSE 0 END) 
        OVER (ORDER BY "Timestamp") AS trade_number
    FROM potential_trades
    WHERE trade_signal = 'Sell'
),
trade_summary AS (
    SELECT 
        trade_number,
        MIN("Timestamp") AS buy_timestamp,
        MAX("Timestamp") AS sell_timestamp,
        MIN("Bid Price") AS buy_price,
        MAX("Bid Price") AS sell_price,
        (MAX("Bid Price") - MIN("Bid Price")) AS profit_loss
    FROM trades
    GROUP BY trade_number
)
SELECT 
    trade_number,
    buy_timestamp,
    sell_timestamp,
    buy_price,
    sell_price,
    profit_loss,
    SUM(profit_loss) OVER (ORDER BY buy_timestamp) AS cumulative_profit
FROM trade_summary
ORDER BY buy_timestamp;



