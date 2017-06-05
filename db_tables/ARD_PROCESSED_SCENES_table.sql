
CREATE TABLE ARD_PROCESSED_SCENES (
   scene_id VARCHAR2(40),
   file_location VARCHAR2(150),
   date_processed date default sysdate,
   processing_state varchar2(15) default 'INQUEUE',
   CONSTRAINT scene_id_pk PRIMARY KEY (scene_id)
);
