CREATE OR REPLACE PROCEDURE MONITORING.MONITORING_SCHEMA.QUERY_EXECUTION_TIME_GROUPS()
RETURNS TABLE ("DATE" DATE, "WAREHOUSE_NAME" VARCHAR, "TIME_GROUP" VARCHAR, "QUERY_ID" VARCHAR, "EXECUTION_SECONDS" FLOAT, "EXECUTION_MINUTES" FLOAT, "COUNT_OF_QUERIES" NUMBER(38,0))
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
 LET results RESULTSET := (
      WITH BUCKETS AS (
     
         SELECT ''Less than 1 minute'' as execution_time_bucket, 0 as execution_time_lower_bound, 60000 as execution_time_upper_bound
         UNION ALL
         SELECT ''1-2 minutes'' as execution_time_bucket, 60000 as execution_time_lower_bound, 120000 as execution_time_upper_bound
         UNION ALL
         SELECT ''2-5 minutes'' as execution_time_bucket, 120000 as execution_time_lower_bound, 300000 as execution_time_upper_bound
         UNION ALL
         SELECT ''5-15 minutes'' as execution_time_bucket, 300000 as execution_time_lower_bound, 900000 as execution_time_upper_bound
         UNION ALL
         SELECT ''15-30 minutes'' as execution_time_bucket, 900000 as execution_time_lower_bound, 1800000 as execution_time_upper_bound
         UNION ALL
         SELECT ''30-60 minutes'' as execution_time_bucket, 1800000 as execution_time_lower_bound, 3600000 as execution_time_upper_bound
         UNION ALL
         SELECT ''more than 60 minutes'' as execution_time_bucket, 3600000 as execution_time_lower_bound, NULL as execution_time_upper_bound
     )
     
     SELECT 
       DATE(START_TIME) as DATE
     , WAREHOUSE_NAME
     , COALESCE(execution_time_bucket,''more than 60 minutes'') as TIME_GROUP
     -- , count(Query_ID) as number_of_queries
     , QUERY_ID
     , TOTAL_ELAPSED_TIME/1000::FLOAT as execution_seconds
     , TOTAL_ELAPSED_TIME/(1000*60)::FLOAT as execution_minutes,
     count(time_group) over(partition by time_group) as count_of_queries 
     from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
     FULL OUTER JOIN BUCKETS B ON (Q.TOTAL_ELAPSED_TIME) >= B.execution_time_lower_bound and (Q.TOTAL_ELAPSED_TIME) < B.execution_time_upper_bound
     where Q.Query_ID is null
         OR (
             TO_DATE(Q.START_TIME) >= DATEADD(month,-4,TO_DATE(CURRENT_TIMESTAMP())) 
             -- and warehouse_name = ''TABLEAU_XLWH''
             -- and warehouse_name = ''COE_SWH''
             -- and TOTAL_ELAPSED_TIME > 0 
             and TOTAL_ELAPSED_TIME/1000 >= 60
         )              
    );

    RETURN TABLE(results);
END;
';