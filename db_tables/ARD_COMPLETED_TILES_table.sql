
CREATE TABLE ARD_COMPLETED_TILES (
   tile_id VARCHAR2(40),
   contributing_scenes VARCHAR2(130),
   date_completed date default sysdate,
   complete_tile char(1),
   processing_state varchar2(15),
   CONSTRAINT tile_id_pk PRIMARY KEY (tile_id)
);
