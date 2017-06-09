CREATE or REPLACE VIEW ARD_UNPROCESSED_SCENES_V AS
select DATE_ACQUIRED, WRS_PATH,WRS_ROW,file_loc,LANDSAT_PRODUCT_ID,satellite,
       case
            when WRS_PATH < 49 then 'CONUS'
            when WRS_PATH||WRS_ROW in (6246,6247,6345,6346,6347,6445,6446,6545,6645) then 'HI'
            when WRS_PATH||WRS_ROW in (7823,7923,7924,8021,8023,8024,8120,8123,8124,8223,8224,8318,8324,8418,8424,8516,8524,8624,8724,8823,8824,8923,8924,9023) then 'NOGRID'
            else 'AK'
       end as REGION from (
SELECT * FROM
  (
  select 
  distinct
  to_date(substr(LANDSAT_PRODUCT_ID,18,8),'YYYYMMDD') as DATE_ACQUIRED,
  to_number(ltrim(substr(LANDSAT_PRODUCT_ID, 11,3),'0')) as WRS_PATH,
  to_number(ltrim(substr(LANDSAT_PRODUCT_ID, 14,3),'0')) as WRS_ROW,
  L2_LOCATION || '/' || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,3) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,4) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,6) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,7) || '-SC*.tar.gz' file_loc,
  LANDSAT_PRODUCT_ID,
  substr(LANDSAT_PRODUCT_ID,1,4) satellite
 from l2_albers_inventory
 where L2_ALBERS_INVENTORY_ID in (select max(L2_ALBERS_INVENTORY_ID) from l2_albers_inventory group by LANDSAT_SCENE_ID)
 minus
 SELECT * FROM
 (
 select
 distinct
  to_date(substr(LANDSAT_PRODUCT_ID,18,8),'YYYYMMDD') as DATE_ACQUIRED,
  to_number(ltrim(substr(LANDSAT_PRODUCT_ID, 11,3),'0')) as WRS_PATH,
  to_number(ltrim(substr(LANDSAT_PRODUCT_ID, 14,3),'0')) as WRS_ROW,
 L2_LOCATION || '/' || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,3) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,4) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,6) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,7) || '-SC*.tar.gz' file_loc,
 LANDSAT_PRODUCT_ID,
 substr(LANDSAT_PRODUCT_ID,1,4) satellite
 from l2_albers_inventory
 where L2_ALBERS_INVENTORY_ID in (select max(L2_ALBERS_INVENTORY_ID) from l2_albers_inventory group by LANDSAT_SCENE_ID)
 )
 where satellite = 'LC08'
 and DATE_ACQUIRED > to_date('12-FEB-2013', 'DD-MON-YYYY')
 and DATE_ACQUIRED < to_date('11-APR-2013', 'DD-MON-YYYY')
 )
 where LANDSAT_PRODUCT_ID not in (select scene_id from ARD_PROCESSED_SCENES)
 union
 SELECT * FROM
 (
 select
 distinct
  to_date(substr(LANDSAT_PRODUCT_ID,18,8),'YYYYMMDD') as DATE_ACQUIRED,
  to_number(ltrim(substr(LANDSAT_PRODUCT_ID, 11,3),'0')) as WRS_PATH,
  to_number(ltrim(substr(LANDSAT_PRODUCT_ID, 14,3),'0')) as WRS_ROW,
 L2_LOCATION || '/' || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,3) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,4) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,6) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,7) || '-SC*.tar.gz' file_loc,
 LANDSAT_PRODUCT_ID,
 substr(LANDSAT_PRODUCT_ID,1,4) satellite
 from l2_albers_inventory
 where L2_ALBERS_INVENTORY_ID in (select max(L2_ALBERS_INVENTORY_ID) from l2_albers_inventory group by LANDSAT_SCENE_ID)
 )
 where LANDSAT_PRODUCT_ID in (select scene_id from ARD_PROCESSED_SCENES where PROCESSING_STATE = 'BLANK')
)
/
