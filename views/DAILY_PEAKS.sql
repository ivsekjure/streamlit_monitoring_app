create or replace view DAILY_PEAKS(
	DATE, YEAR, MONTHH, WEEK, HOUR, WAREHOUSE_NAME, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
) as 
 WITH data as(
         SELECT 
               DATE(START_TIME) as  DATE
             , YEAR(START_TIME)::INT as YEAR
             , MONTH(START_TIME) as MONTHH
             , LPAD(WEEK(START_TIME),2 , '0')::INT as WEEK
             , LPAD(DAY(START_TIME),2 , '0')::INT as DAY
             , LPAD(HOUR(START_TIME), 2, '0') as HOUR
             , WAREHOUSE_NAME
             , sum(CREDITS_USED_COMPUTE) as CREDITS_USED_COMPUTE
             FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
         GROUP BY 1,2,3,4,5,6,7
         ORDER BY 1,2,3,4,5
     )
     select *
     from data
         PIVOT(sum(CREDITS_USED_COMPUTE) FOR DAY in(01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 14, 15
         , 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31)) as d;