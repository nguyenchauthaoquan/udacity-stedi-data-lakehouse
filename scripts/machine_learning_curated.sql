CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-lakehouse`.`machine_learning_curated` (
  `timestamp` bigint,
  `x` float,
  `y` float,
  `z` float,
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'FALSE',
    'dots.in.keys' = 'FALSE',
    'case.insensitive' = 'TRUE',
    'mapping' = 'TRUE'
)
         STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://quannguyenchauthao-stedi-lakehouse/machine_learning/curated/'
TBLPROPERTIES ('classification' = 'json');