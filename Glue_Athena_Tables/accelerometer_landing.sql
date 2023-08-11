CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_landing`(
  `user` string COMMENT 'email address', 
  `timeStamp` bigint COMMENT 'from deserializer', 
  `x` float COMMENT 'from deserializer', 
  `y` float COMMENT 'from deserializer', 
  `z` float COMMENT 'from deserializer'
  )
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'serialisation.format'='1') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-aws-project/landing/accelerometer'
TBLPROPERTIES (
  'classification'='json', 
  'has_encrypted_data'='FALSE', 
  'transient_lastDdlTime'='1691484394')