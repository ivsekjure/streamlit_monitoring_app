create or replace view TOP_50_LONGEST_QUERIES(
	YEAR_MONTH_WEEK,
	WAREHOUSE_NAME,
	QUERY_ID,
	QUERY_TEXT,
	USER_NAME,
	QUERY_EXECUTION_TIME_MINUTES,
	PARTITIONS_SCANNED,
	PARTITIONS_TOTAL,
	PCN_SCANNED,
	EXECUTION_TIME_MINUTES,
	RNK
) as 
// Top 50 Longest Running Queries by week
/* Is there an opportunity to optimize with clustering or upsize the warehouse? */
with data as(
 select
       YEAR(START_TIME) ||'-'|| LPAD(MONTH(START_TIME),2 , '0') ||'-'|| LPAD(WEEK(START_TIME),2 , '0') as YEAR_MONTH_WEEK
     , WAREHOUSE_NAME
     , QUERY_ID
     , QUERY_TEXT
     , USER_NAME
     , TOTAL_ELAPSED_TIME/(1000*60) AS QUERY_EXECUTION_TIME_MINUTES
     , PARTITIONS_SCANNED
     , PARTITIONS_TOTAL
     , case when PARTITIONS_TOTAL is not null and PARTITIONS_TOTAL > 0 then PARTITIONS_SCANNED / PARTITIONS_TOTAL else 0 end as PCN_SCANNED
 from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
 where 1=1
     and TO_DATE(START_TIME) >     DATEADD(month,-5,TO_DATE(CURRENT_TIMESTAMP())) 
     and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
     and ERROR_CODE is NULL
     and PARTITIONS_SCANNED is not null
 order by  TOTAL_ELAPSED_TIME desc
)
select *, sum(QUERY_EXECUTION_TIME_MINUTES) over(partition by YEAR_MONTH_WEEK, USER_NAME) as EXECUTION_TIME_MINUTES,
 dense_rank() over(partition by YEAR_MONTH_WEEK order by QUERY_EXECUTION_TIME_MINUTES desc) as rnk
from data
qualify rnk <= 50;