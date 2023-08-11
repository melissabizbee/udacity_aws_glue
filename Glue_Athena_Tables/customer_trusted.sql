CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_trusted`(
  `customerName` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer', 
  `birthDay` string COMMENT 'from deserializer', 
  `serialNumber` string COMMENT 'from deserializer', 
  `registrationDate` bigint COMMENT 'from deserializer', 
  `lastupdateDate` bigint COMMENT 'from deserializer', 
  `shareWithResearchAsOfDate` bigint COMMENT 'from deserializer', 
  `shareWithPublicAsOfDate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'serialisation.format'='1') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-aws-project/trusted/customer'
TBLPROPERTIES (
  'classification'='json', 
  'has_encrypted_data'='FALSE', 
  'transient_lastDdlTime'='1691484394')