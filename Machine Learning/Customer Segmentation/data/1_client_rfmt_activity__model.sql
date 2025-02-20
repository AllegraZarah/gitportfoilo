-- Extract key client transaction metrics from trading data
WITH transactions AS (
    SELECT 
        trans.client_id,  
        -- Calculate client transaction tenure in years and months
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, MIN(trans.trade_timestamp))) AS client_transaction_tenure_years,
        (EXTRACT(YEAR FROM AGE(CURRENT_DATE, MIN(trans.trade_timestamp))) * 12) + 
        (EXTRACT(MONTH FROM AGE(CURRENT_DATE, MIN(trans.trade_timestamp)))) AS trade_tenure_months,

        -- Count total trades and recent trade activity in the last 90 days
        COUNT(trans.record_id) AS trade_freq, 
        COUNT(CASE WHEN (CURRENT_DATE - DATE(trans.trade_timestamp)) <= 90 THEN trans.record_id END) AS last90days_trade_freq,

        -- Breakdown of trade frequency by Purchase and Disposal types
        COUNT(CASE WHEN trans.transaction_type = 'Acquire' THEN trans.record_id END) AS buy_trade_freq,
        COUNT(CASE WHEN trans.transaction_type = 'Release' THEN trans.record_id END) AS sell_trade_freq,
        COUNT(DISTINCT trans.transaction_type) AS trade_type,  -- Number of unique trade types

        -- Total trade value and recency of last trade
        SUM(trans.adjusted_order_value) AS trade_value,
        CURRENT_DATE - MAX(DATE(trans.trade_timestamp)) AS days_since_last_trade,
        (EXTRACT(YEAR FROM AGE(CURRENT_DATE, MAX(trans.trade_timestamp))) * 12) + 
        (EXTRACT(MONTH FROM AGE(CURRENT_DATE, MAX(trans.trade_timestamp)))) AS trade_recency_months,

        -- Number of different security types traded
        COUNT(DISTINCT trans.asset_type) AS distinct_boards_traded

    FROM public.stg_fact_marketflows trans  -- Updated schema name

    -- Filter for reconciled trades and valid system providers
    WHERE trans.trade_status_summary = 'Reconciled' 
          AND (trans.system_name IS NULL OR trans.system_name = 'X4')
    
    GROUP BY trans.client_id
),

-- Extract loan-related insights per client
loan AS (
    SELECT 
        client.client_id, 
        SUM(loan.loan_value) AS loan_value_collected,  
        COUNT(DISTINCT loan.transaction_id) AS loan_freq,  

        -- Calculate loan tenure and recency
        (EXTRACT(YEAR FROM AGE(CURRENT_DATE, MIN(loan.deal_timestamp))) * 12) + 
        (EXTRACT(MONTH FROM AGE(CURRENT_DATE, MIN(loan.deal_timestamp)))) AS loan_tenure_months,
        (EXTRACT(YEAR FROM AGE(CURRENT_DATE, MAX(loan.deal_timestamp))) * 12) + 
        (EXTRACT(MONTH FROM AGE(CURRENT_DATE, MAX(loan.deal_timestamp)))) AS loan_recency_months

    FROM public.stg_client_loans loan
    LEFT JOIN public.dim_client client
    ON loan.client_id = client.id

    GROUP BY client.client_id
),

-- Extract deposit and withdrawal activity per client
deposit_withdrawal AS (
    SELECT 
        cli.client_id,

        -- Summarize deposit frequency, amounts, and recency
        SUM(CASE WHEN transaction_origin = 'Funding' THEN amount END) AS deposit_amount,
        COUNT(CASE WHEN transaction_origin = 'Funding' THEN client_id END) AS deposit_freq,
        COUNT(CASE WHEN (transaction_origin = 'Funding' AND ((CURRENT_DATE - DATE(wal.created)) <= 90)) THEN client_id END) AS last90days_deposit_freq,
        (EXTRACT(YEAR FROM AGE(CURRENT_DATE, MIN(CASE WHEN transaction_origin = 'Funding' THEN wal.created END))) * 12) + 
        (EXTRACT(MONTH FROM AGE(CURRENT_DATE, MIN(CASE WHEN transaction_origin = 'Funding' THEN wal.created END)))) AS deposit_tenure_months,
        (EXTRACT(YEAR FROM AGE(CURRENT_DATE, MAX(CASE WHEN transaction_origin = 'Funding' THEN wal.created END))) * 12) + 
        (EXTRACT(MONTH FROM AGE(CURRENT_DATE, MAX(CASE WHEN transaction_origin = 'Funding' THEN wal.created END)))) AS deposit_recency_months,

        -- Summarize withdrawal frequency, amounts, and recency
        SUM(CASE WHEN transaction_origin = 'PayoutRequest' THEN amount END) AS withdrawal_amount,
        COUNT(CASE WHEN transaction_origin = 'PayoutRequest' THEN client_id END) AS withdrawal_freq,
        COUNT(CASE WHEN (transaction_origin = 'PayoutRequest' AND ((CURRENT_DATE - DATE(wal.created)) <= 90)) THEN client_id END) AS last90days_withdrawal_freq,
        (EXTRACT(YEAR FROM AGE(CURRENT_DATE, MIN(CASE WHEN transaction_origin = 'PayoutRequest' THEN wal.created END))) * 12) + 
        (EXTRACT(MONTH FROM AGE(CURRENT_DATE, MIN(CASE WHEN transaction_origin = 'PayoutRequest' THEN wal.created END)))) AS withdrawal_tenure_months,
        (EXTRACT(YEAR FROM AGE(CURRENT_DATE, MAX(CASE WHEN transaction_origin = 'PayoutRequest' THEN wal.created END))) * 12) + 
        (EXTRACT(MONTH FROM AGE(CURRENT_DATE, MAX(CASE WHEN transaction_origin = 'PayoutRequest' THEN wal.created END)))) AS withdrawal_recency_months

    FROM public.stg_wallet_transactions wal
    LEFT JOIN public.dim_client cli
    ON wal.client_id = cli.id

    -- Filter to include only relevant transaction types
    WHERE ((transaction_origin = 'Funding') OR (transaction_origin = 'PayoutRequest' AND transaction_type = 'Cleared Debit'))

    GROUP BY cli.client_id
),

-- Extract wallet balance details per client
wallet_balance AS (
    SELECT 
        wal.client_id, 
        wal.available_balance, 
        wal.cash_advance_balance
    FROM public.stg_client_wallet wal
    WHERE system_name = 'NXMARKET' AND payment_currency = 'USD'
)

-- Final aggregation: Combine client details with financial activity
SELECT 
    cli.client_id, 
    cli.client_type, 
    cli.user_type,
    
    -- Calculate overall client tenure
    (EXTRACT(YEAR FROM AGE(CURRENT_DATE, cli.created)) * 12) + 
    (EXTRACT(MONTH FROM AGE(CURRENT_DATE, cli.created))) AS client_tenure_months,

    -- Transactional activity
    trans.trade_freq, 
    trans.last90days_trade_freq,
    trans.buy_trade_freq,
    trans.sell_trade_freq,
    trans.trade_recency_months,
    trans.trade_tenure_months,
    trans.trade_value,

    -- Wallet balance
    wal.available_balance,
    wal.cash_advance_balance,

    -- Deposit activity
    dpw.deposit_freq,
    dpw.last90days_deposit_freq,
    dpw.deposit_recency_months,
    dpw.deposit_tenure_months,
    dpw.deposit_amount,

    -- Withdrawal activity
    dpw.withdrawal_freq,
    dpw.last90days_withdrawal_freq,
    dpw.withdrawal_recency_months,
    dpw.withdrawal_tenure_months,
    dpw.withdrawal_amount,

    -- Loan activity
    loan.loan_freq,
    loan.loan_recency_months,
    loan.loan_tenure_months,
    loan.loan_value_collected

FROM public.dim_client cli
LEFT JOIN transactions trans ON cli.client_id = trans.client_id
LEFT JOIN deposit_withdrawal dpw ON cli.client_id = dpw.client_id
LEFT JOIN wallet_balance wal ON cli.client_id = wal.client_id
LEFT JOIN loan loan ON cli.client_id = loan.client_id

-- Sort results by client ID for structured output
ORDER BY cli.client_id;