SELECT_ALL =
select cast(work_user_id as numeric) user_id, work_name name, work_address address
 from etl_merge_input_work_entity


SELECT_ALL_WITH_RANGE=
select work_user_id user_id, work_name name, work_address address
 from etl_merge_input_work_entity where line_number between ? and ?

SELECT_ALL_WITH_RANGE_AND_FILTER=
select work_user_id user_id, work_name name, work_address address
 from etl_merge_input_work_entity where line_number between ? and ?
 and work_user_id not in(3, 4)
