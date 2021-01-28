create table kafka_offline_source (
  content VARCHAR
) PROPERTIES (
  category = 'source',
  type = 'kafka',
  auto_offset_reset = 'latest',
  version = '0.10.1.0',
  delimiter = 'raw',
  topic = 'sys-offline',
  brokers = 'c-og6qlzk01e.kafka.cn-east-3.internal:9092',
  group_id = 'device_offline_status_0002'
);

create table kafka_online_source (
  content VARCHAR
) PROPERTIES (
  category = 'source',
  type = 'kafka',
  version = '0.10.1.0',
  delimiter = 'raw',
  topic = 'sys-online',
  brokers = 'c-og6qlzk01e.kafka.cn-east-3.internal:9092',
  group_id = 'device_online_status_0001'
);

create table device_status_tmp(
  device_name VARCHAR,
  device_status VARCHAR,
  report_time BIGINT
)
PROPERTIES( category = 'tmp');



insert into device_status_tmp
select
  JSONPATH(content, '$.deviceName') as device_name,
  '1' as device_status,
  JSONPATH(content, '$.timestamp') as report_time
from kafka_online_source
where JSONPATH(content,'$.topic') like 'hello/%';

insert into device_status_tmp
select
  JSONPATH(content, '$.deviceName') as device_name,
  '0' as device_status,
  JSONPATH(content, '$.timestamp') as report_time
from kafka_offline_source
where JSONPATH(content,'$.topic') like 'will/%';

create table device_status_sink (
  device_name VARCHAR,
  device_status VARCHAR,
  report_time BIGINT,
  time_stamp BIGINT
) PROPERTIES (
  category = 'sink',
    type = 'tsdb',
    tsdb_url = '192.168.8.16:8091,192.168.8.17:8091,192.168.8.15:8091',
    tsdb_user = 'netease_monitor',
    tsdb_password = 'ISs0wKrFpP',
    tsdb_database = 'light_of_thing',
    tsdb_table = 'iot_device_status',
    tsdb_tags = 'device_name',
    tsdb_time_field = 'time_stamp',
    tsdb_time_unit = 'MILLISECONDS',
    buffer_size = '1',
    FLUSH_INTERVAL = '1000'
);

insert into device_status_sink
select s.device_name,s.device_status,s.report_time,s.report_time
from device_status_tmp s