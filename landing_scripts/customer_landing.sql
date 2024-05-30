CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string,
  `email` string, 
  `phone` string, 
  `birthday` string,
  `serialnumber` string,
  `registrationdate` bigint,
  `lastupdatedate` bigint, 
  `sharewithresearchasofdate` bigint, 
  `sharewithpublicasofdate` bigint, 
  `sharewithfriendsasofdate` bigint
 )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://tsp-lakehouse/customer/landing/'
TBLPROPERTIES ('classification'='json')