CREATE or REPLACE VIEW ARD_L2_ALBERS_INVENTORY_V AS
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
/
