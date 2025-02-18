create or replace view AUTOSUSPEND_COSTS(
	DATE,
	WAREHOUSE_NAME,
	CREDITS_USED_TOTAL,
	CREDIT_USED_FOR_RESUME,
	PCN_AUTORESUME_COST
) as
   WITH 
     WH_SIZE AS
     (
          SELECT WAREHOUSE_SIZE, NODES
            FROM (
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
                 )
     ),
     WH_consumption as(
         SELECT  
             --   YEAR(START_TIME) ||'-'|| LPAD(MONTH(START_TIME),2 , '0') as DATE
               DATE(START_TIME) as DATE
             , NAME
             , SERVICE_TYPE
             , SUM(CREDITS_USED) as CREDITS_USED_TOTAL
         FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
         WHERE SERVICE_TYPE = 'WAREHOUSE_METERING'
         GROUP BY 1,2,3
         ORDER BY 1,2,3 DESC 
     ),
     WH_LIST as( ---here we need to add all releveant WHs
       select 'AZADEA_WH' as WAREHOUSE_NAME, 'X-SMALL' as WAREHOUSE_SIZE, 120 as AUTO_SUSPEND
       union all
       select 'COMPUTE_WH' as WAREHOUSE_NAME, 'X-SMALL' as WAREHOUSE_SIZE, 120 as AUTO_SUSPEND
       union all
       select 'S_COMPUTE' as WAREHOUSE_NAME, 'SMALL' as WAREHOUSE_SIZE, 120 as AUTO_SUSPEND
     ),
     WH_EVENTS_HIST as(
         select
             --   YEAR(WEH.TIMESTAMP) ||'-'|| LPAD(MONTH(WEH.TIMESTAMP),2 , '0') as DATE
               DATE(WEH.TIMESTAMP) as DATE
             , WEH.WAREHOUSE_ID
             , WEH.WAREHOUSE_NAME
             , sum(case when WEH.EVENT_NAME = 'SUSPEND_CLUSTER' and EVENT_REASON = 'WAREHOUSE_AUTOSUSPEND' then 1 else 0 end) as SUSPEND_CNT
         from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY as WEH
         where WEH.CLUSTER_NUMBER is not null
         group by 1,2,3
         order by 1,2
     ),
     WH_USED_TOTAL as(
       select
             WEH.DATE
           -- , WEH.WAREHOUSE_ID
           , WEH.WAREHOUSE_NAME
           -- , WS.WAREHOUSE_SIZE
           -- , WS.NODES
           -- , WC.CREDITS_USED_COMPUTE_SUM
           -- , WC.CREDITS_USED_CLOUD_SUM
           -- , WEH.SUSPEND_CNT
           , sum(WC.CREDITS_USED_TOTAL) as CREDITS_USED_TOTAL
           , sum(((WEH.SUSPEND_CNT*WL.AUTO_SUSPEND)/60/60) * WS.NODES) as CREDIT_USED_FOR_RESUME
           -- , (CREDIT_USED_FOR_RESUME / CREDITS_USED_TOTAL) * 100 as PCN_RESUME_VS_TOTAL
       from WH_EVENTS_HIST as WEH
       left join WH_LIST as WL on(WL.WAREHOUSE_NAME = WEH.WAREHOUSE_NAME)
       left join WH_SIZE as WS on(WS.WAREHOUSE_SIZE = WL.WAREHOUSE_SIZE)
       left join WH_consumption as WC on(WC.NAME = WEH.WAREHOUSE_NAME and WC.DATE = WEH.DATE)
       where WS.WAREHOUSE_SIZE is not null
       group by 1,2
       order by 1
     )
     select DATE
         , WAREHOUSE_NAME
         , CREDITS_USED_TOTAL
         , CREDIT_USED_FOR_RESUME
         , ((CREDIT_USED_FOR_RESUME / CREDITS_USED_TOTAL) * 100)::number(38,1) as PCN_AUTORESUME_COST
     from WH_USED_TOTAL
     ;