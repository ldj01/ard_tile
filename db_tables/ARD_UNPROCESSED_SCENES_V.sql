CREATE or REPLACE VIEW ARD_UNPROCESSED_SCENES_V AS
SELECT * FROM
  (
  select
  distinct
  trunc(coalesce(b.DATE_ACQUIRED,c.DATE_ACQUIRED,d.DATE_ACQUIRED)) as DATE_ACQUIRED,
  coalesce(b.WRS_PATH,c.WRS_PATH,d.WRS_PATH) WRS_PATH,
  coalesce(b.WRS_ROW,c.WRS_ROW,d.WRS_ROW) WRS_ROW,
  a.L2_LOCATION || '/' || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,3) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,4) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,6) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,7) || '-SC*.tar.gz' file_loc,
  a.LANDSAT_PRODUCT_ID LANDSAT_PRODUCT_ID,
 substr(a.LANDSAT_PRODUCT_ID,1,4) satellite
 from l2_bridge.l2_albers_inventory a
 left join etm_scene_inventory@inv_l2_bridge_link b on a.landsat_scene_id = b.landsat_scene_id and b.vcid = 1
 left join tm_scene_inventory@inv_l2_bridge_link c on a.landsat_scene_id = c.landsat_scene_id
 LEFT JOIN inventory.lmd_scene d on A.LANDSAT_SCENE_ID = d.landsat_scene_id
 where a.L2_ALBERS_INVENTORY_ID in (select max(L2_ALBERS_INVENTORY_ID) from l2_bridge.l2_albers_inventory group by LANDSAT_SCENE_ID)
 minus
 SELECT * FROM
 (
 select
 distinct
 trunc(d.DATE_ACQUIRED) as DATE_ACQUIRED,
 d.WRS_PATH WRS_PATH,
 d.WRS_ROW WRS_ROW,
 a.L2_LOCATION || '/' || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,3) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,4) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,6) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,7) || '-SC*.tar.gz' file_loc,
 a.LANDSAT_PRODUCT_ID LANDSAT_PRODUCT_ID,
 substr(a.LANDSAT_PRODUCT_ID,1,4) satellite
 from l2_bridge.l2_albers_inventory a
 LEFT JOIN inventory.lmd_scene d on A.LANDSAT_SCENE_ID = d.landsat_scene_id
 where a.L2_ALBERS_INVENTORY_ID in (select max(L2_ALBERS_INVENTORY_ID) from l2_bridge.l2_albers_inventory group by LANDSAT_SCENE_ID)
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
 trunc(coalesce(b.DATE_ACQUIRED,c.DATE_ACQUIRED,d.DATE_ACQUIRED)) as DATE_ACQUIRED,
 coalesce(b.WRS_PATH,c.WRS_PATH,d.WRS_PATH) WRS_PATH,
 coalesce(b.WRS_ROW,c.WRS_ROW,d.WRS_ROW) WRS_ROW,
 a.L2_LOCATION || '/' || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,3) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,4) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,6) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,7) || '-SC*.tar.gz' file_loc,
 a.LANDSAT_PRODUCT_ID LANDSAT_PRODUCT_ID,
 substr(a.LANDSAT_PRODUCT_ID,1,4) satellite
 from l2_bridge.l2_albers_inventory a
 left join etm_scene_inventory@inv_l2_bridge_link b on a.landsat_scene_id = b.landsat_scene_id and b.vcid = 1
 left join tm_scene_inventory@inv_l2_bridge_link c on a.landsat_scene_id = c.landsat_scene_id
 LEFT JOIN inventory.lmd_scene d on A.LANDSAT_SCENE_ID = d.landsat_scene_id
 where a.L2_ALBERS_INVENTORY_ID in (select max(L2_ALBERS_INVENTORY_ID) from l2_bridge.l2_albers_inventory group by LANDSAT_SCENE_ID)
 )
 where LANDSAT_PRODUCT_ID in (select scene_id from ARD_PROCESSED_SCENES where PROCESSING_STATE = 'BLANK');

