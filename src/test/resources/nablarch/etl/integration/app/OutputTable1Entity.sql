SELECT_INPUT_FILE1=
SELECT
  user_id,
  name
FROM
  input_file1_table

SELECT_OUTPUT_TABLE1_FROM_INPUT_FILE3=
SELECT
  user_id,
  name
FROM
  input_file3_table
  
  
SELECT_INPUT_FILE1_WITH_RANGE=
SELECT
  user_id,
  name
FROM
  input_file1_table
where line_number between ? and ?
