-- Table: fact_client_daily_balance
-- Purpose: Tracks daily account balances for clients


-- DDL for fact_client_daily_balance table creation
-- Option 1: Full table refresh (when schema remains unchanged)
DELETE FROM public_mart.fact_client_daily_balance

-- Option 2: Table recreation (when schema changes needed)
-- DROP TABLE public_mart.fact_client_daily_balance;

-- CREATE TABLE public_mart.fact_client_daily_balance (
-- 				date DATE, 
-- 				client_id VARCHAR(25),  
-- 				account_balance DOUBLE PRECISION, 
-- 				latest_trans_at TIMESTAMP
-- 	);



-- Backfill Query: Updates fact_client_daily_balance with historic records
INSERT INTO public_mart.fact_client_daily_balance (date, client_id, account_balance, latest_trans_at)


WITH transactions AS(
        -- Extract transactions with details and assign a row index to each transaction per client and day
	SELECT created_at
            , updated_at
            , client_id
            , start_amount
			, end_amount
            , amount
            , transaction_type
            , transaction_status
			, ROW_NUMBER() OVER (PARTITION BY client_id, DATE(created_at) ORDER BY created_at DESC) row_index 
                -- To get the last transaction for each client for each day. Notice the date function in the partition clause, it is to ensure the partition is by the entire day and not a timestamp
	
	FROM public.transactions_log

    WHERE transaction_status NOT IN ('Declined', 'Reverted')
	),

balance_per_trans AS(
        -- Retrieve only the last transaction for each client per day
	SELECT *
			, DATE(created_at) balance_date
	
	FROM transactions
	
	WHERE row_index = 1
        -- This where clause ensures only the last transaction for each client per day is retrieved, other transactions apart from this per day are not relevant for this part of the analysis
	),
	

date_dim AS (
        -- Generate a date range for the required period (adjustable based on business needs)
	SELECT date_actual
	
	FROM public_mart.dim_date
    
    WHERE date_actual between '2023-01-01' AND (current_date - 1)
	),

client_start_date AS (
        -- Determine the first transaction date for each client
    SELECT client_id
        	, DATE(MIN(created_at)) AS start_date
	
    FROM transactions
    GROUP BY client_id
	),

clients_and_dates AS (
        -- Generate a date series for each client, starting from their first transaction date
    SELECT client_start_date.*
            , date_dim.date_actual
	
    FROM client_start_date
    LEFT JOIN date_dim
    ON client_start_date.start_date <= date_dim.date_actual
	),
	
balance_per_day AS(
        -- Now we join this date series for each client to the balances and total amount table, to get the balance for the days they did transact, however we are not done yet.
        -- We need to get the latest transaction value for days with no transactions after the client started transacting, and we require and helper column for that, for which we created running total field
        -- This field would be concatenated with the client_id to create a unique identifier which would be used in identifying the latest transaction at each level. This is done in the following cte
	SELECT cad.date_actual date
			, cad.client_id actual_client_id -- This client_id is the actual (think final) because that from the balance table would have null values for days without transaction for each client
			, bal.*
			, SUM(CASE WHEN start_amount IS NULL THEN 0 ELSE 1 END)
                    OVER (PARTITION BY cad.client_id
							 ORDER BY cad.date_actual) running_total_transactions
	
	FROM clients_and_dates cad
	LEFT JOIN balance_per_trans bal
		ON cad.date_actual = bal.balance_date
		AND cad.client_id = bal.client_id
	),

unique_identifier AS (
        -- Add a unique identifier to associate transactions with no-transaction days to the last known balance
	SELECT *
			, concat(actual_client_id, '_', running_total_transactions) unique_balance_identifier
                -- Here is the unique identifier to identify each transaction day to enable the reversion to the last known balance, for days where there are no transactions

	FROM balance_per_day
		),

final AS (
        -- Compute the account balance for all days, using the last known balance for days without transactions
	SELECT date
            , actual_client_id AS client_id
			, first_value(end_amount) OVER (PARTITION BY unique_balance_identifier
												 ORDER BY client_id) account_balance
			, first_value(created_at) OVER (PARTITION BY unique_balance_identifier
												 ORDER BY client_id) latest_trans_at

	FROM unique_identifier
	)

-- Final selection of records to insert into the fact table	
SELECT date
        , client_id
        , account_balance
        , latest_trans_at
	
FROM final
	ORDER BY client_id, date DESC