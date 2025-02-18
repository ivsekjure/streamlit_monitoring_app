create or replace view USER_GROUPS_ANALYSIS(
	YEAR_MONTH,
	YEAR_MONTH_WEEK,
	ROLE_NAME,
	USER_NAME,
	WAREHOUSE_NAME,
	CLUSTER_NUMBER,
	QUERY_TYPE,
	EXECUTION_TIME_M,
	EXECUTION_TIME_H,
	CREDITS_USED_COMPUTE,
	CREDITS_USED_CLOUD_SERVICES
) as
 WITH 
     WH_SIZES AS
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
     WH_LIST as( -- add all relevant WHs
        select 'AZADEA_WH' as WAREHOUSE_NAME, 'X-SMALL' as WAREHOUSE_SIZE, 120 as AUTO_SUSPEND
       union all
       select 'COMPUTE_WH' as WAREHOUSE_NAME, 'X-SMALL' as WAREHOUSE_SIZE, 120 as AUTO_SUSPEND
       union all
       select 'S_COMPUTE' as WAREHOUSE_NAME, 'SMALL' as WAREHOUSE_SIZE, 120 as AUTO_SUSPEND
     )
     select YEAR(QH.START_TIME) ||'-'|| LPAD(MONTH(QH.START_TIME),2 , '0') as YEAR_MONTH
          , YEAR(QH.START_TIME) ||'-'|| LPAD(MONTH(QH.START_TIME),2 , '0') ||'-'|| LPAD(WEEK(QH.START_TIME),2 , '0') as YEAR_MONTH_WEEK
          , QH.ROLE_NAME
          , QH.USER_NAME
          , upper(QH.WAREHOUSE_NAME) as WAREHOUSE_NAME
          , QH.CLUSTER_NUMBER
          , QH.QUERY_TYPE
          , sum(QH.EXECUTION_TIME/(1000*60)) as EXECUTION_TIME_M
          , sum(QH.EXECUTION_TIME/(1000*60*60)) as EXECUTION_TIME_H
          , sum((QH.EXECUTION_TIME/(1000*60*60)) * WS.NODES) as CREDITS_USED_COMPUTE
          , sum(QH.CREDITS_USED_CLOUD_SERVICES) as CREDITS_USED_CLOUD_SERVICES
          
     from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY QH
     left join WH_LIST as GGL on(GGL.WAREHOUSE_NAME = upper(QH.WAREHOUSE_NAME))
     left join WH_SIZES as WS on(WS.WAREHOUSE_SIZE = GGL.WAREHOUSE_SIZE)
     where EXECUTION_STATUS = 'SUCCESS'
     group by 1,2,3,4,5,6,7
     -- limit 100;
     ;