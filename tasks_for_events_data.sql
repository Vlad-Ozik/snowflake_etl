use database TASK1;
use role accountadmin;

alter task if exists del_dublicates_json suspend;
alter task if exists events_task suspend;
alter pipe if exists events_loader_pipe set pipe_execution_paused = true;


CREATE OR REPLACE TASK del_dublicates_json
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('EVENTS_STREAM')
    AS
INSERT overwrite INTO json_events_data
SELECT DISTINCT * FROM json_events_data;


CREATE OR REPLACE TASK events_task
    WAREHOUSE = COMPUTE_WH
    AFTER del_dublicates_json
    AS
MERGE INTO events as e
USING(
    select
    v:event_data:data:eventData:app_user_id::int as player_id,
    v:event_data:data:eventData:platformAccountId::int as device_id,
    v:event_data:timestampClient::int as install_date,
    v:event_data:platform::string as client_id,
    v:event_data:appName::string as app_name,
    v:event_data:countryCode::string as country
    from json_events_data
    where v:event_data:data:eventData:eventType like 'server_install'
) AS jed ON e.player_id = jed.player_id
WHEN MATCHED THEN UPDATE
SET e.player_id = jed.player_id,
    e.device_id = jed.device_id,
    e.install_date = jed.install_date,
    e.client_id = jed.client_id,
    e.app_name = jed.app_name,
    e.country = jed.country
WHEN NOT MATCHED
THEN INSERT
(player_id, device_id, install_date, client_id, app_name, country)
VALUES
(jed.player_id, jed.device_id, jed.install_date, jed.client_id, jed.app_name, jed.country);


SHOW TASKS;

alter task if exists events_task resume;
alter task if exists del_dublicates_json resume;
alter pipe if exists events_loader_pipe set pipe_execution_paused = false;
