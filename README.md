# presto-udfs
presto自定义函数
## 常用函数
  
  Here is a part of common functions.
  
  >string functions
  >>compress(VARCHAR) -> VARCHAR: gzip compress and base64 encode  
  >>uncompress(VARCHAR) -> VARCHAR: base64 decode and gzip uncompress  
  >>md5(VARCHAR) -> VARCHAR & sha256(VARCHAR) -> VARCHAR & sha512(VARCHAR) -> VARCHAR: get the str hash(md5 or sha256 or sha512)   
  >>hybird_encode(VARCHAR) -> VARCHAR: complex hash(sha512 -> base64 -> md5)   
  >>pinyin(VARCHAR) -> VARCHAR: convert chinese to pinyin	  
  
  >date functions
  >>date_format(VARCHAR, VARCHAR) -> VARCHAR: get the date format string   
  >>to_date(VARCHAR) -> VARCHAR: get the date part of the date string   
  >>get_date(BIGINT) -> VARCHAR: get the date compared with today. get_date(-1) or get(1)   
  >>time_diff(VARCHAR,VARCHAR) -> BIGINT: the seconds between two date-time syntax   
  >>unix_timestamp(VARCHAR [,VARCHAR]) -> BIGINT: get unix timestamp   
  
  >JSON functions
  >> is_json_str(VARCHAR) -> BOOLEAN: check the string is json type  
  >> parse_json_keys(VARCHAR) -> array(varchar): get json keys  
  >> get_json_object(varchar, varchar) -> varchar & parse_json_object(varchar, varchar) -> varchar: parse the json to varchar(x) by key  
  >> parse_json_array(VARCHAR) -> array(varchar): parse the json array to array  
  >> json_array_extract(json, jsonPath) -> array(varchar): extract json array by given jsonPath.  
  
  >other functions
  >> typeOfDay(VARCHAR) -> BIGINT: get the day of week from a date string. 1-holiday、2-weekend、3-weekday、4-workday（work overtime）  
  
## 使用
  >1.build the project if needed.  
  >> mvn clean install  
  
  >2.put presto-udfs-{version}-shaded.jar into $PRESTO_HOME/plugin/hive-hadoop2  
  >> mv presto-udfs-1.0.0-shaded.jar $PRESTO_HOME/plugin/hive-hadoop2  
  
  >3.restart presto server  
  >> $PRESTO_HOME/bin/launcher restart  
  
