CREATE OR REPLACE PROCEDURE MONITORING.MONITORING_SCHEMA.QUERY_NUM_OF_EXECUTIONS()
RETURNS TABLE ("DATE" DATE, "QUERY_TEXT" VARCHAR, "WAREHOUSE_NAME" VARCHAR, "USER_CNT" NUMBER(38,0), "NUMBER_OF_QUERIES" NUMBER(38,0), "EXECUTION_SECONDS" FLOAT, "EXECUTION_MINUTES" FLOAT, "EXECUTION_HOURS" FLOAT)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
 LET results RESULTSET := (
            SELECT
          DATE(START_TIME) as DATE
         ,case when len(query_text) = 0 THEN ''/'' ELSE QUERY_TEXT END as QUERY_TEXT
         ,coalesce(WAREHOUSE_NAME, ''MISSING'') as WAREHOUSE_NAME
         ,count(distinct USER_NAME) as USER_CNT
         ,count(*) as number_of_queries
         ,(sum(TOTAL_ELAPSED_TIME)/1000)::float as execution_seconds
         ,(sum(TOTAL_ELAPSED_TIME)/(1000*60))::float as execution_minutes
         ,(sum(TOTAL_ELAPSED_TIME)/(1000*60*60))::float as execution_hours
        from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        where 1=1
         and TO_DATE(START_TIME) > DATEADD(month,-6,TO_DATE(CURRENT_TIMESTAMP())) 
         and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
         and QUERY_TYPE = ''SELECT''
         and query_text <> ''''
        group by all
        having count(*) >= 10 --configurable/minimal threshold
    );

    RETURN TABLE(results);
END;
';