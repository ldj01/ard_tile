CREATE or REPLACE VIEW ARD_UNPROCESSED_SCENES_V AS
select DATE_ACQUIRED, WRS_PATH,WRS_ROW,file_loc,LANDSAT_PRODUCT_ID,satellite,
       case
            when WRS_PATH < '049' then 'CONUS'
            when WRS_PATH||WRS_ROW in ('062046','062047','063045','063046','063047','064045','064046','065045','066045') then 'HI'
            when WRS_PATH||WRS_ROW in ('078023','079023','079024','080021','080023','080024','081020','081023','081024','082023','082024','083018','083024','084018','084024','085016','085024','086024','087024','088023','088024','089023','089024','090023') then 'NOGRID'
            else 'AK'
       end as REGION from (
SELECT * FROM
  (
  select 
  distinct
  to_date(substr(LANDSAT_PRODUCT_ID,18,8),'YYYYMMDD') as DATE_ACQUIRED,
  substr(LANDSAT_PRODUCT_ID, 11,3) as WRS_PATH,
  substr(LANDSAT_PRODUCT_ID, 14,3) as WRS_ROW,
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
  substr(LANDSAT_PRODUCT_ID, 11,3) as WRS_PATH,
  substr(LANDSAT_PRODUCT_ID, 14,3) as WRS_ROW,
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
  substr(LANDSAT_PRODUCT_ID, 11,3) as WRS_PATH,
  substr(LANDSAT_PRODUCT_ID, 14,3) as WRS_ROW,
 L2_LOCATION || '/' || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,3) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,4) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,6) || regexp_substr(LANDSAT_PRODUCT_ID, '[^_]+',1,7) || '-SC*.tar.gz' file_loc,
 LANDSAT_PRODUCT_ID,
 substr(LANDSAT_PRODUCT_ID,1,4) satellite
 from l2_albers_inventory
 where L2_ALBERS_INVENTORY_ID in (select max(L2_ALBERS_INVENTORY_ID) from l2_albers_inventory group by LANDSAT_SCENE_ID)
 )
 where LANDSAT_PRODUCT_ID in (select scene_id from ARD_PROCESSED_SCENES where PROCESSING_STATE = 'BLANK')
)
/
