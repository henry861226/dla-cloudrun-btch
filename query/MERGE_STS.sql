MERGE `{{ projectId }}.{{ dataset }}.JOB_STS` AS target
USING (
  SELECT '{{ current_date }}' AS exec_date, {{status}} AS status
) AS source
ON target.exec_date = source.exec_date
WHEN MATCHED THEN
  UPDATE SET target.status = source.status
WHEN NOT MATCHED THEN
  INSERT (exec_date, status) VALUES (source.exec_date, source.status)