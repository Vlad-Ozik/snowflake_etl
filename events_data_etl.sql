/*
   player_id - app_user_id
   device_id - platformAccountId
   install_date - timestampClient
   client_id - platform
   app_name - appName
   country - countryCode
*/

CREATE OR REPLACE TABLE events
(
  player_id integer,
  device_id integer,
  install_date integer,
  client_id string,
  app_name string,
  country string
);

CREATE OR REPLACE STORAGE INTEGRATION task_sf
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('<my_gcs_folder>')
  
LIST @TASK_SF_GCS

CREATE OR REPLACE TABLE json_events_data (v variant);

COPY INTO json_events_data FROM @TASK_SF_GCS  //TASK_SF_GCS it's the my STAGE
file_format=(type=json);

DESC STORAGE INTEGRATION task_sf;

CREATE NOTIFICATION INTEGRATION gcp_notification
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = '<my_gcs_sub>';

DESC NOTIFICATION INTEGRATION gcp_notification;

CREATE OR REPLACE PIPE events_loader_pipe
  AUTO_INGEST = true
  INTEGRATION = gcp_notification
  AS
COPY INTO json_events_data
  FROM  @TASK_SF_GCS;

CREATE OR REPLACE STREAM events_stream ON TABLE json_events_data;

CREATE OR REPLACE TASK events_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '2 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('EVENTS_STREAM')
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

ALTER TASK events_task RESUME;
