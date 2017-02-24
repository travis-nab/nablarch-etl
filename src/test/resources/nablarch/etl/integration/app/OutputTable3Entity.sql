SELECT_INPUT_FILE1_AND_INPUT_FILE2=
SELECT
  user_id,
  name,
  col2,
  cast(col3 as numeric)
FROM
  input_file1_table,
  input_file2_table
WHERE
  input_file1_table.user_id = input_file2_table.col1