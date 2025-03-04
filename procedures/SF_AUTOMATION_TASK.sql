CREATE OR REPLACE PROCEDURE MONITORING.MONITORING_SCHEMA.SF_AUTOMATION_TASK()
RETURNS TABLE ("DATE" DATE, "QUERY_HASH" VARCHAR, "QUERY_TEXT" VARCHAR, "NAME" VARCHAR, "WAREHOUSE_NAME" VARCHAR, "WAREHOUSE_SIZE" VARCHAR, "NODES" NUMBER(38,0), "DATABASE_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "EXE_COUNT" NUMBER(38,0), "DURATION_HOURS" FLOAT, "CREDITS_USED" FLOAT, "CREDITS_USED" NUMBER(38,0))
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

   EXECUTE IMMEDIATE ''SHOW WAREHOUSES'';
   LET results RESULTSET := (
    with 
    WH_SIZES as (
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
    ),
    TASK_WH as(
        select
              NAME 
            , DATABASE_NAME
            , OWNER
            , WAREHOUSE_NAME
            , SCHEDULE
            , GRAPH_VERSION
            , LAST_COMMITTED_ON
            , DENSE_RANK() OVER (partition by NAME order by LAST_COMMITTED_ON desc) as RNK_DATE
            , DENSE_RANK() OVER (partition by NAME order by GRAPH_VERSION desc) as RNK_GRAPH
        from SNOWFLAKE.ACCOUNT_USAGE.TASK_VERSIONS
    ),
    TASK_WH_11 as(select * from TASK_WH where RNK_DATE = 1 and RNK_GRAPH = 1),
    TASK_WH_REST as(select * from TASK_WH where NAME not in(select distinct NAME from TASK_WH_11)),
    TASK_WH_FINAL as(select * from TASK_WH_11 union all select * from TASK_WH_REST where RNK_GRAPH = 1),
    FINAL as(
        select DATE(TH.QUERY_START_TIME) as DATE
            , TH.QUERY_HASH
            , TH.QUERY_TEXT
            , TH.NAME
            , TW.WAREHOUSE_NAME
            , WL."size" as WAREHOUSE_SIZE
            , WS.NODES
            , TH.DATABASE_NAME
            , TH.SCHEMA_NAME
            --  , TH.STATE
            , count(*) as EXE_COUNT
            , sum(DATEDIFF(minutes, TH.QUERY_START_TIME, TH.COMPLETED_TIME)::number(38,2) / 60)::float as DURATION_HOURS
        from snowflake.account_usage.task_history as TH
        left join TASK_WH_FINAL as TW on(TW.NAME = TH.NAME)
        left join TABLE(RESULT_SCAN(LAST_QUERY_ID())) as WL on(TW.WAREHOUSE_NAME = WL."name")
        left join WH_SIZES as WS on(WS.WAREHOUSE_SIZE = WL."size")
        where TH.QUERY_START_TIME >= DATEADD (month, -12, CURRENT_TIMESTAMP())
            and TH.STATE in(''SUCCEEDED'', ''CANCELLED'')
        group by all
    )
    select *
         , (DURATION_HOURS * NODES)::float as CREDITS_USED
         , 1 as credits_used_2
    from FINAL
    );

    RETURN TABLE(results);
END;
';