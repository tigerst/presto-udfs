# presto-udfs
presto自定义函数
## 常用函数

  >string functions
  >>str_split: split the str by separatorSlice and len 
  >>substring_index: get substring by delim and count 
  >>compress: gzip compress and base64 encode 
  >>uncompress: base64 decode and gzip uncompress 
  >>md5 & sha256 & sha512: get the str hash(md5 or sha256 or sha512) 
  >>hybird_encode: complex hash(sha512 -> base64 -> md5) 
  
  >date functions
  >>date_format: get the date format string 
  >>to_date: get the date part of the date string 
  >>get_date: get the date compared with today. get_date(-1) or get(1) 
  >>time_diff: the seconds between two date-time syntax 
  >>unix_timestamp: get unix timestamp 
  
  >JSON functions
  >> is_json_str: check the string is json type  
  >> parse_json_keys: get json keys  
  >> get_json_object & parse_json_object: parse the json to varchar(x) by key  
  >> parse_json_array: parse the json to array(varchar)  
  
## 使用
