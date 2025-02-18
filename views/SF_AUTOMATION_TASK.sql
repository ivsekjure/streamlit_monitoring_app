create or replace view SF_AUTOMATION_TASK(
	DATE,
	QUERY_HASH,
	QUERY_TEXT,
	NAME,
	WAREHOUSE_NAME,
	WAREHOUSE_SIZE,
	NODES,
	DATABASE_NAME,
	SCHEMA_NAME,
	EXE_COUNT,
	DURATION_HOURS,
	CREDITS_USED,
	CREDITS_USED_2
) as
    with 
    GGL_WH_LIST as(
       -- add all relevant WHs
        select 'STANDARD_XS' as WAREHOUSE_NAME, 'X-SMALL' as WAREHOUSE_SIZE, 120 as AUTO_SUSPEND
       union all
       select 'SNOWPARK_XS' as WAREHOUSE_NAME, 'X-SMALL' as WAREHOUSE_SIZE, 120 as AUTO_SUSPEND
    ),
    WH_SIZES as(
        SELECT 'X-SMALL' AS WAREHOUSE_SIZE, 1 AS NODES
        UNION ALL
        SELECT 'SMALL' AS WAREHOUSE_SIZE, 2 AS NODES
        UNION ALL
        SELECT 'MEDIUM' AS WAREHOUSE_SIZE, 4 AS NODES
        UNION ALL
        SELECT 'LARGE' AS WAREHOUSE_SIZE, 8 AS NODES
        UNION ALL
        SELECT 'X-LARGE' AS WAREHOUSE_SIZE, 16 AS NODES
        UNION ALL
        SELECT '2X-LARGE' AS WAREHOUSE_SIZE, 32 AS NODES
        UNION ALL
        SELECT '3X-LARGE' AS WAREHOUSE_SIZE, 64 AS NODES
        UNION ALL
        SELECT '4X-LARGE' AS WAREHOUSE_SIZE, 128 AS NODES
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
        -- where WAREHOUSE_NAME is not null
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
            , GWL.WAREHOUSE_SIZE
            , WS.NODES
            , TH.DATABASE_NAME
            , TH.SCHEMA_NAME
            --  , TH.STATE
            , count(*) as EXE_COUNT
            , sum(DATEDIFF(minutes, TH.QUERY_START_TIME, TH.COMPLETED_TIME)::number(38,2) / 60) as DURATION_HOURS
        from snowflake.account_usage.task_history as TH
        left join TASK_WH_FINAL as TW on(TW.NAME = TH.NAME)
        left join GGL_WH_LIST as GWL on(TW.WAREHOUSE_NAME = GWL.WAREHOUSE_NAME)
        left join WH_SIZES as WS on(WS.WAREHOUSE_SIZE = GWL.WAREHOUSE_SIZE)
        where TH.QUERY_START_TIME >= DATEADD (month, -12, CURRENT_TIMESTAMP())
            and TH.STATE in('SUCCEEDED', 'CANCELLED')
        group by 1,2,3,4,5,6,7,8,9
        order by DATE
    )
    select *
         , DURATION_HOURS * NODES as CREDITS_USED
         , 1 as credits_used_2
    from FINAL;
