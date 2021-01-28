CREATE TABLE source (
  content VARCHAR
) PROPERTIES (
  category = 'source',
  type = 'kafka',
  auto_offset_reset = 'earliest',
  delimiter = 'raw',
  version = '0.11.0.3',
  topic = 'IOT-xA707NOn1Lh1D4r',
  brokers = 'c-og6qlzk01e.kafka.cn-east-3.internal:9092',
  group_id = 'SLOTH-0414-01'
);

CREATE TABLE sink (
  id  VARCHAR,
  mthd  VARCHAR,
  ts  BIGINT,
  area  VARCHAR,
  cluster VARCHAR,
  device_name  VARCHAR,
  num  VARCHAR,
  time_dur  INTEGER,
  energy_dur INTEGER,
  energy double,
  pow double,
  acquisition_time BIGINT,
  nwk_id VARCHAR,
  version VARCHAR,
  uuid VARCHAR
) PROPERTIES (
  category = 'sink',
  type = 'tsdb',
  tsdb_url = '192.168.8.16:8091,192.168.8.17:8091,192.168.8.15:8091',
  tsdb_user = 'netease_monitor',
  tsdb_password = 'ISs0wKrFpP',
  tsdb_database = 'light_of_thing',
  tsdb_table = 'lamp_consumption',
  tsdb_tags = 'id,mthd,area,cluster,num,device_name,version,nwk_id,uuid',
  tsdb_time_field = 'ts',
  tsdb_time_unit = 'MILLISECONDS',
  buffer_size = '1000',
  FLUSH_INTERVAL = '1000'
);
create table tmp(
  productKey varchar,
  deviceName varchar,
  topicId varchar,
  topicType varchar,
  topic varchar,
  qos varchar,
  content varchar,
  messageId varchar,
  time_seq BIGINT)
 PROPERTIES( category = 'tmp');

 insert into tmp select
 JSONPATH(content,'$.productKey'),
  JSONPATH(content,'$.deviceName'),
  JSONPATH(content,'$.topicId'),
  JSONPATH(content,'$.topicType'),
  JSONPATH(content,'$.topic'),
  JSONPATH(content,'$.qos'),
  BASE64DECODE(JSONPATH(content,'$.content')),
  JSONPATH(content,'$.messageId'),
  JSONPATH(content,'$.timestamp')
  from source;

insert into sink select
JSONPATH(content, '$.id'),
JSONPATH(content, '$.method'),
JSONPATH(content, '$.params.time'),
JSONPATH(content, '$.params.value.area'),
JSONPATH(content, '$.params.value.cluster'),
JSONPATH(content, '$.params.value.device_name'),
JSONPATH(content, '$.params.value.number'),
JSONPATH(content, '$.params.value.time_dur'),
JSONPATH(content, '$.params.value.energy_dur'),
JSONPATH(content, '$.params.value.energy'),
JSONPATH(content, '$.params.value.power'),
JSONPATH(content, '$.params.value.acquisition_time'),
JSONPATH(content, '$.params.value.nwk_id'),
JSONPATH(content, '$.version'),
JSONPATH(content, '$.params.value.uuid')
from tmp where topic like 'xA707NOn1Lh1D4r/%/user/event/consumption/post';