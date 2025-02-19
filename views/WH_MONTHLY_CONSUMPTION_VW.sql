create or replace view WH_MONTHLY_CONSUMPTION_VW(
	YEAR_MONTH,
	NAME,
	SERVICE_TYPE,
	SERVICE_GROUP,
	CREDITS_USED_COMPUTE_SUM,
	CREDITS_USED_CLOUD_SERVICES,
	CREDITS_USED
) as
// MONTHLY WH CONSUMPTION COMPUTE
SELECT  YEAR(START_TIME) ||'-'|| LPAD(MONTH(START_TIME),2 , '0') as YEAR_MONTH
   , NAME
   , SERVICE_TYPE
   , case when SERVICE_TYPE = 'WAREHOUSE_METERING' then 'WAREHOUSE_METERING' else 'OTHER' end as SERVICE_GROUP
   , SUM(CREDITS_USED_COMPUTE) AS CREDITS_USED_COMPUTE_SUM
   , SUM(CREDITS_USED_CLOUD_SERVICES) as CREDITS_USED_CLOUD_SERVICES
   , SUM(CREDITS_USED) as CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
GROUP BY 1,2,3,4
ORDER BY 1,2,3 DESC
;