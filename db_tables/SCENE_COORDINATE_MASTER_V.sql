CREATE OR REPLACE FORCE VIEW L2_BRIDGE.SCENE_COORDINATE_MASTER_V
(
   LANDSAT_SCENE_ID,
   LANDSAT_PRODUCT_ID,
   CORNER_LL_LAT,
   CORNER_LL_LON,
   CORNER_LR_LAT,
   CORNER_LR_LON,
   CORNER_UL_LAT,
   CORNER_UL_LON,
   CORNER_UR_LAT,
   CORNER_UR_LON
)
   BEQUEATH DEFINER
AS
   (SELECT DISTINCT a.landsat_scene_id,
                    b.landsat_product_id,
                    TO_NUMBER (CORNER_LL_LAT) CORNER_LL_LAT,
                    TO_NUMBER (CORNER_LL_LON) CORNER_LL_LON,
                    TO_NUMBER (CORNER_LR_LAT) CORNER_LR_LAT,
                    TO_NUMBER (CORNER_LR_LON) CORNER_LR_LON,
                    TO_NUMBER (CORNER_UL_LAT) CORNER_UL_LAT,
                    TO_NUMBER (CORNER_UL_LON) CORNER_UL_LON,
                    TO_NUMBER (CORNER_UR_LAT) CORNER_UR_LAT,
                    TO_NUMBER (CORNER_UR_LON) CORNER_UR_LON
      FROM ll0arcb.etm_scene_inventory@inv_l2_bridge_link a
           JOIN l2_bridge.l2_albers_inventory b
              ON a.landsat_scene_id = b.landsat_scene_id
    UNION
    SELECT DISTINCT b.landsat_scene_id,
                    b.landsat_product_id,
                    TO_NUMBER (CORNER_LL_LAT) CORNER_LL_LAT,
                    TO_NUMBER (CORNER_LL_LON) CORNER_LL_LON,
                    TO_NUMBER (CORNER_LR_LAT) CORNER_LR_LAT,
                    TO_NUMBER (CORNER_LR_LON) CORNER_LR_LON,
                    TO_NUMBER (CORNER_UL_LAT) CORNER_UL_LAT,
                    TO_NUMBER (CORNER_UL_LON) CORNER_UL_LON,
                    TO_NUMBER (CORNER_UR_LAT) CORNER_UR_LAT,
                    TO_NUMBER (CORNER_UR_LON) CORNER_UR_LON
      FROM ll0arcb.tm_scene_inventory@inv_l2_bridge_link a
           JOIN l2_bridge.l2_albers_inventory b
              ON a.landsat_scene_id = b.landsat_scene_id
    UNION
    SELECT DISTINCT c.landsat_scene_id,
                    c.landsat_product_id,
                    TO_NUMBER (CORNER_LL_LAT) CORNER_LL_LAT,
                    TO_NUMBER (CORNER_LL_LON) CORNER_LL_LON,
                    TO_NUMBER (CORNER_LR_LAT) CORNER_LR_LAT,
                    TO_NUMBER (CORNER_LR_LON) CORNER_LR_LON,
                    TO_NUMBER (CORNER_UL_LAT) CORNER_UL_LAT,
                    TO_NUMBER (CORNER_UL_LON) CORNER_UL_LON,
                    TO_NUMBER (CORNER_UR_LAT) CORNER_UR_LAT,
                    TO_NUMBER (CORNER_UR_LON) CORNER_UR_LON
      FROM inventory.lmd_l1_scene_coordinate a
           JOIN inventory.lmd_scene b ON a.lmd_scene_id = b.lmd_scene_id
           JOIN L2_BRIDGE.L2_ALBERS_INVENTORY c
              ON c.landsat_scene_id = b.landsat_scene_id);
