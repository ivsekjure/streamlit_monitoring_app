CREATE OR REPLACE PROCEDURE MONITORING.MONITORING_SCHEMA.USER_GROUPS_ANALYSIS()
RETURNS TABLE ("YEAR_MONTH" VARCHAR, "YEAR_MONTH_WEEK" VARCHAR, "ROLE_NAME" VARCHAR, "USER_NAME" VARCHAR, "WAREHOUSE_NAME" VARCHAR, "CLUSTER_NUMBER" NUMBER(38,0), "QUERY_TYPE" VARCHAR, "EXECUTION_TIME_M" FLOAT, "EXECUTION_TIME_H" FLOAT, "CREDITS_USED_COMPUTE" FLOAT, "CREDITS_USED_CLOUD_SERVICES" FLOAT)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

 EXECUTE IMMEDIATE ''SHOW WAREHOUSES'';

 
 LET results RESULTSET := (
   
    WITH 
     WH_SIZES AS
     (
          SELECT WAREHOUSE_SIZE, NODES
            FROM (
                   SELECT ''X-Small'' AS WAREHOUSE_SIZE, 1 AS NODES
                   UNION ALL
                   SELECT ''Small'' AS WAREHOUSE_SIZE, 2 AS NODES
                   UNION ALL
                   SELECT ''Medium'' AS WAREHOUSE_SIZE, 4 AS NODES
                   UNION ALL
                   SELECT ''Large'' AS WAREHOUSE_SIZE, 8 AS NODES
                   UNION ALL
                   SELECT ''X-Large'' AS WAREHOUSE_SIZE, 16 AS NODES
                   UNION ALL
                   SELECT ''2X-Large'' AS WAREHOUSE_SIZE, 32 AS NODES
                   UNION ALL
                   SELECT ''3X-Large'' AS WAREHOUSE_SIZE, 64 AS NODES
                   UNION ALL
                   SELECT ''4X-Large'' AS WAREHOUSE_SIZE, 128 AS NODES
                 )
     ),
     WH_LIST as (
        select "name" as WAREHOUSE_NAME, "size" as WAREHOUSE_SIZE
        from table(result_scan(last_query_id()))
     )
     select YEAR(QH.START_TIME) ||''-''|| LPAD(MONTH(QH.START_TIME),2 , ''0'') as YEAR_MONTH
          , YEAR(QH.START_TIME) ||''-''|| LPAD(MONTH(QH.START_TIME),2 , ''0'') ||''-''|| LPAD(WEEK(QH.START_TIME),2 , ''0'') as YEAR_MONTH_WEEK
          , QH.ROLE_NAME as ROLE_NAME
          , QH.USER_NAME as USER_NAME
          , upper(QH.WAREHOUSE_NAME) as WAREHOUSE_NAME
          , QH.CLUSTER_NUMBER as CLUSTER_NUMBER
          , QH.QUERY_TYPE as QUERY_TYPE
          , CAST(sum(QH.EXECUTION_TIME/(1000*60)) as float) as EXECUTION_TIME_M
          , CAST(sum(QH.EXECUTION_TIME/(1000*60*60)) as float) as EXECUTION_TIME_H
          , CAST(sum((QH.EXECUTION_TIME/(1000*60*60)) * WS.NODES) as float) as CREDITS_USED_COMPUTE
          , CAST(sum(QH.CREDITS_USED_CLOUD_SERVICES) as float) as CREDITS_USED_CLOUD_SERVICES
          
     from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY QH
     left join WH_LIST as WL on(WL.WAREHOUSE_NAME = upper(QH.WAREHOUSE_NAME))
     left join WH_SIZES as WS on(WS.WAREHOUSE_SIZE = WL.WAREHOUSE_SIZE)
     where EXECUTION_STATUS = ''SUCCESS''
     group by all
    );

    RETURN TABLE(results);
END;
';