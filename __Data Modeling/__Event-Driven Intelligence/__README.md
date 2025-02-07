# Event-Driven Intelligence Models –
### Unlocking behavioral and financial insights through real-time event tracking.

This directory contains backfill models to create historic daily balance tables. These models are run one time and do not require any orchestartions. The directory also contains scripts to incrementally update this daily balance records. The dag script to enable airflow orchestration is also included in the directory.


## Tools Utilized
- SQL (DDL, DQL) 
- Python
- Linux Command Line Interface***
- Airflow
- Postgres Management Tool

_______________________________________

Below are the list of models contained in the Event-Driven Intelligence directory._

## Actor Performance: A Dimensional Approach
One row in the actors_history_scd table (final table) represents a historical record of an actor’s performance classification and activity status for a specific period.

Understanding trends in actor performance and industry activity requires structured historical data. The raw actor_films dataset lacked an optimized model for efficient analysis, making it difficult to track performance trends, categorize actors, and support incremental updates. This project addresses these challenges by modeling actor data, enabling classification, and maintaining historical records. The final table enables tracking changes over time by implementing Type 2 Slowly Changing Dimensions (SCD), where each record has a start_date and end_date.

#### Key Components
- actors Table: Stores an actor’s film history, performance classification (star, good, average, bad), and activity status.
- actors_history_scd Table: Implements Type 2 Slowly Changing Dimension (SCD) to track historical changes in actor performance and activity.
- Queries: Includes cumulative updates, backfilling, and incremental updates to maintain data integrity.

#### Advanced SQL Concepts Adopted
- User-Defined Composite Types (CREATE TYPE ... AS (...))
- User-Defined Enumerated Types (ENUMs) (CREATE TYPE ... AS ENUM(...))
- Array Data Type (films[])
- Composite Primary Key (PRIMARY KEY (actor_id, year))
- Windows Function (... OVER (PARTITION BY ... ORDER BY ...))
- Array Aggregation (ARRAY_AGG(...))
- Array Element Removal (ARRAY_REMOVE(...))
- Array Cardinality & Indexing (CARDINALITY(films_details))
- Type Casting and Field Extraction from Composite Types ((films_details[...]::films).year)
- Unnesting Arrays of Composite Types (UNNEST(ARRAY[ ROW(...)::actor_scd_type, ROW(...)::actor_scd_type ]))
- Extracting Fields from Composite Types ((records::actor_scd_type).quality_class)


## Digital Presence: Modeling User & Host Events
One row in the host_activity_reduced table (final table) represents the summarized activity of a specific host for a given month. It consolidates daily event data into a more efficient format, making it easier to analyze long-term trends in host engagement.

Tracking user and host activity over time is essential for understanding engagement trends and system performance. However, raw event data is often unstructured and difficult to analyze efficiently. This project models device and host activity by structuring key event-based metrics, enabling cumulative tracking, and supporting incremental updates.

#### Key Components
- user_devices_cumulated Table: Aggregates user activity by browser type, tracking active dates either as a structured map (MAP<STRING, ARRAY[DATE]>) or as multiple rows per user.*******
- hosts_cumulated Table: Captures historical host activity by logging active dates.
- host_activity_reduced Table: A monthly fact table summarizing host engagement, including total hits and unique visitors.
- Queries: Includes deduplication, cumulative and incremental processing, and transformation of date-based activity tracking into optimized formats.

#### Advanced SQL Concepts Used
-- *********** (To be manually added: window functions, array operations, map structures, incremental logic, etc.)



## Account Balance (Backfill, Update Script) - 
One record of this table shows the account balance of an individual client for each day since the client started transacting with the entity.




## Clients Portfolio Balances (Backfill, Update Script) - 
One record of this table shows the current amount of each individual product each client has in their portfolio for each day since the client started transacting with the entity.



### Inventory Position