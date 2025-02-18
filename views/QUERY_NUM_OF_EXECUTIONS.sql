create or replace view QUERY_NUM_OF_EXECUTIONS(
	DATE,
	QUERY_TEXT,
	WAREHOUSE_NAME,
	USER_CNT,
	NUMBER_OF_QUERIES,
	EXECUTION_SECONDS,
	EXECUTION_MINUTES,
	EXECUTION_HOURS
) as
// Queries by  of Times Executed and Execution Time
/* Are there any queries that get executed a ton?? how much execution time do they take up? */
SELECT
  -- YEAR(START_TIME) ||'-'|| LPAD(MONTH(START_TIME),2 , '0') as YEAR_MONTH
  DATE(START_TIME) as DATE
 ,case when len(query_text) = 0 THEN '/' ELSE QUERY_TEXT END as QUERY_TEXT
 ,coalesce(WAREHOUSE_NAME, 'MISSING') as WAREHOUSE_NAME
 ,count(distinct USER_NAME) as USER_CNT
 ,count(*) as number_of_queries
 ,sum(TOTAL_ELAPSED_TIME)/1000 as execution_seconds
 ,sum(TOTAL_ELAPSED_TIME)/(1000*60) as execution_minutes
 ,sum(TOTAL_ELAPSED_TIME)/(1000*60*60) as execution_hours
from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
where 1=1
 and TO_DATE(START_TIME) > DATEADD(month,-6,TO_DATE(CURRENT_TIMESTAMP())) 
 and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
 and QUERY_TYPE = 'SELECT'
 -- and WAREHOUSE_NAME is not null -- queries which did not use cache
group by 1,2,3
having count(*) >= 10 --configurable/minimal threshold
order by 1, 3 desc
-- limit 100 --configurable upper bound threshold
;