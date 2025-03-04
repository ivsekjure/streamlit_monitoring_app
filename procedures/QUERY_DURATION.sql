CREATE OR REPLACE PROCEDURE MONITORING.MONITORING_SCHEMA.QUERY_DURATION()
RETURNS TABLE ("START_TIME" DATE, "WAREHOUSE_NAME" VARCHAR, "QUERY_TEXT" VARCHAR, "USER_NAME" VARCHAR, "EXECUTION_SECONDS" FLOAT, "EXECUTION_MINUTES" FLOAT, "EXECUTION_HOURS" FLOAT)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
 LET results RESULTSET := (
     SELECT 
        START_TIME::DATE as START_TIME
        , WAREHOUSE_NAME
        , QUERY_TEXT
        , USER_NAME
        , TOTAL_ELAPSED_TIME/1000::FLOAT as execution_seconds
        , TOTAL_ELAPSED_TIME/(1000*60)::FLOAT as execution_minutes
        , TOTAL_ELAPSED_TIME/(1000*60*60)::FLOAT as execution_hours
    from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    where user_name <> ''SYSTEM''
    
    );

    RETURN TABLE(results);
END;
';