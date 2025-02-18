create or replace view QUERY_DURATION(
	START_TIME,
	WAREHOUSE_NAME,
	QUERY_TEXT,
	USER_NAME,
	EXECUTION_SECONDS,
	EXECUTION_MINUTES,
	EXECUTION_HOURS
) as
 /* set warehouse_name = 'COMPUTE_WH';
set execution_start = '2024-06-18';
set duration_in_minutes = 1;
 */
SELECT 
    START_TIME::DATE as START_TIME
    , WAREHOUSE_NAME
    , QUERY_TEXT
    , USER_NAME
    , TOTAL_ELAPSED_TIME/1000 as execution_seconds
    , TOTAL_ELAPSED_TIME/(1000*60) as execution_minutes
    , TOTAL_ELAPSED_TIME/(1000*60*60) as execution_hours
from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
where true 
  /*   and WAREHOUSE_NAME = $warehouse_name
  and DATE(START_TIME) = $execution_start
    and TOTAL_ELAPSED_TIME/(1000*60) >= 1; */
;