/*
   player_id - это app_user_id
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
  STORAGE_ALLOWED_LOCATIONS = ('gcs://...')
  
LIST @TASK_SF_GCS

CREATE OR REPLACE TABLE json_events_data (v variant);

COPY INTO json_events_data FROM @TASK_SF_GCS
file_format=(type=json);

DESC STORAGE INTEGRATION task_sf;


CREATE NOTIFICATION INTEGRATION gcp_notification
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = '...';

DESC NOTIFICATION INTEGRATION gcp_notification;


CREATE OR REPLACE PIPE events_loader_pipe
  AUTO_INGEST = true
  INTEGRATION = gcp_notification
  AS
COPY INTO json_events_data
  FROM  @TASK_SF_GCS;

CREATE OR REPLACE STREAM events_stream ON TABLE json_events_data APPEND_ONLY = TRUE;
