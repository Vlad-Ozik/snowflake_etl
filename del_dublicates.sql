use database TASK1;
use role accountadmin;

alter task if exists del_dublicates_json suspend;
alter task if exists events_task suspend;
alter pipe if exists events_loader_pipe set pipe_execution_paused = true;


insert overwrite into events
select distinct * from events;

insert overwrite into json_events_data
select distinct * from json_events_data;


alter task if exists events_task resume;
alter task if exists del_dublicates_json resume;
alter pipe if exists events_loader_pipe set pipe_execution_paused = false;
