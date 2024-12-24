SELECT status
FROM `{{ projectId }}.{{ dataset }}.JOB_STS`
WHERE exec_date = '{{ current_date }}'