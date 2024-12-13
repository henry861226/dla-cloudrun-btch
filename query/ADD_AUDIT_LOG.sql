-- 檢查目標表是否存在
IF NOT EXISTS (
  SELECT 1 
  FROM `{{ projectId}}.{{ dataset }}.INFORMATION_SCHEMA.TABLES` 
  WHERE table_name = 'AUDIT_LOG'
) THEN
  -- 創建目標表
  EXECUTE IMMEDIATE """
  CREATE TABLE `{{ projectId }}.{{ dataset }}.AUDIT_LOG`
    (
      create_datetime STRING,
      cust_uuid STRING,
      cust_tags ARRAY<STRING>,
      group_uuid STRING,
      group_tags ARRAY<STRING>
    )
  """;
END IF;

-- 新增本次批次結果
INSERT INTO `{{ projectId }}.{{ dataset }}.AUDIT_LOG`
  SELECT 
    '{{ datetime }}' AS create_datetime,
    cust_grp_map.cust_uuid,
    cust_tags.tags,
    cust_grp_map.group_uuid,
    grp_meta.tags
  FROM
    `{{ projectId }}.{{ dataset }}.CUST_GRP_MAP_{{ date }}` AS cust_grp_map
  LEFT JOIN
    `{{ projectId }}.{{ dataset }}.CUST_TAGS_{{ date }}` AS cust_tags
  ON
    cust_grp_map.cust_uuid = cust_tags.cust_uuid
  LEFT JOIN
    `{{ dataset }}.GROUP_META` AS grp_meta
  ON
    cust_grp_map.group_uuid = grp_meta.group_uuid