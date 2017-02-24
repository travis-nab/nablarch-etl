SELECT_ALL =
select USER_ID USER_ID,last_name || ' ' || first_name as NAME,ADDRESS ADDRESS from etl_work_table

SELECT_ALL_WITH_RANGE =
select USER_ID USER_ID,last_name || ' ' || first_name as NAME,ADDRESS ADDRESS from etl_work_table where line_number between ? and ?
