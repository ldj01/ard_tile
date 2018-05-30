""" Methods for pre-defined database interactions """

import cx_Oracle

from util import logger


def connect(connstr):
    """ Open a new Oracle DB connection

    Args:
        connstr (str): Oracle formatted connection string

    Returns:
        Connection: Oracle connection

    Example:
        >>> connection('schema/secret@host:port/database')
        <cx_Oracle.Connection to schema/secret@host:port/database>
    """
    try:
        return cx_Oracle.connect(connstr)
    except:
        logger.exception('Unable to connect to database!')
        raise


def select(connection, sql, **opts):
    """ Execute select statement and return results list

    Args:
        connection (cx_Oracle.Connection): open database connection object
        sql (str): oracle sql select statement with optional args (e.g. :key1)
        opts (dict): key/value for optional args in select statement

    Returns:
        list: rows returned as key/value dicts

    Example:
        >>> select(db, 'SELECT ACQUIRED FROM TABLE WHERE PATH = :path', path='035')
        [{'ACQUIRED': datetime.datetime(1989, 7, 12, 0, 0)}]
    """
    cursor = connection.cursor()
    logger.debug('Select SQL Statement...')
    cursor.execute(sql, opts)
    logger.debug('Select SQL Statement: %s', cursor.statement)
    columns = [i[0] for i in cursor.description]
    return [dict(zip(columns, row)) for row in cursor]


def update(connection, sql, **opts):
    """ Execute SQL statement and commit changes

    Args:
        connection (cx_Oracle.Connection): open database connection object
        sql (str): oracle sql select statement with optional args (e.g. :key1)
        opts (dict): key/value for optional args in select statement

    Returns:
        None
    """
    cursor = connection.cursor()
    logger.debug('Commit SQL Statement...')
    cursor.execute(sql, opts)
    logger.debug('Commit SQL Statement: %s', cursor.statement)
    connection.commit()


def update_many(connection, sql, array=None):
    """ Execute SQL statement with array of rows, commit changes

    Args:
        connection (cx_Oracle.Connection): open database connection object
        sql (str): oracle sql select statement with optional args (e.g. :key1)
        array (list): all key/values for optional args in select statement

    Returns:
        None
    """
    cursor = connection.cursor()
    cursor.bindarraysize = len(array)
    cursor.prepare(sql)
    logger.debug('Execute Many SQL Statement...')
    cursor.executemany(None, array)
    logger.debug('Execute Many SQL Statement: %s', cursor.statement)
    connection.commit()


def update_scene_state(connection, scene_id, state):
    """ Update the state of a scene record in the ARD_PROCESSED_SCENES table """
    ard_processed_scenes_update = ("update ARD_PROCESSED_SCENES set PROCESSING_STATE = :state, "
                                   "DATE_PROCESSED = sysdate where scene_id = :scene_id")
    update(connection, ard_processed_scenes_update, state=state, scene_id=scene_id)


def insert_tile_record(connection, completed_tile_list):
    """ Insert a tile record into the ARD_COMPLETED_TILES table """
    processed_tiles_insert = ("insert /*+ ignore_row_on_dupkey_index(ARD_COMPLETED_TILES, TILE_ID_PK) */ "
                              "into ARD_COMPLETED_TILES (tile_id,CONTRIBUTING_SCENES,COMPLETE_TILE,PROCESSING_STATE) "
                              "values (:1,:2,:3,:4)")
    update_many(connection, processed_tiles_insert, completed_tile_list)


def select_consecutive_scene(connection, acqdate, wrsrow, wrspath, n=2, **kwargs):
    """ Select same-day scene ids from consective WRS_ROWs along a fixed WRS_PATH """
    ls_prod_id_sql = ("select distinct LANDSAT_SCENE_ID from "
                      "  inventory.LMD_SCENE where "
                      "  trunc(DATE_ACQUIRED) = to_date(:acqdate,'YYYYMMDD') "
                      "  and LANDSAT_PRODUCT_ID is not null "
                      "  and WRS_ROW >= :minrow and WRS_ROW <= :maxrow "
                      "  and WRS_PATH = :wrspath")
    results = select(connection, ls_prod_id_sql,
                     acqdate=acqdate, minrow=int(wrsrow)-n, maxrow=int(wrsrow)+n,
                     wrspath=int(wrspath))
    return [r['LANDSAT_SCENE_ID'] for r in results]


def select_corner_polys(connection, scene_ids):
    """ Get coordinates for input scene list """
    poly_intersect_sql =("select LANDSAT_PRODUCT_ID, "
                         " 'POLYGON ((' ||  CORNER_UL_LON || ' ' || CORNER_UL_LAT || ',' || "
                         " CORNER_LL_LON || ' ' || CORNER_LL_LAT || ',' || "
                         " CORNER_LR_LON || ' ' || CORNER_LR_LAT || ',' || "
                         " CORNER_UR_LON || ' ' || CORNER_UR_LAT || ',' || "
                         " CORNER_UL_LON || ' ' || CORNER_UL_LAT || '))' AS COORDS "
                         " from SCENE_COORDINATE_MASTER_V where LANDSAT_SCENE_ID in ({})"
                         " order by LANDSAT_PRODUCT_ID desc")
    # Oracle does not support binding for IN
    poly_intersect_sql = poly_intersect_sql.format(",".join(["'%s'" %x  for x in scene_ids]))
    return select(connection, poly_intersect_sql)


def check_tile_status(connection, tile_id):
    """ Fetch information about a tile id """
    tile_status_sql = ("select tile_id, contributing_scenes, complete_tile from ARD_COMPLETED_TILES"
                       " where tile_id = :tile_id")
    return select(connection, tile_status_sql, tile_id=tile_id)


def fetch_file_loc(connection, sat, wildcard):
    """ Wildcard search for File Locations by mission and glob """
    wildcard = '%{w}%'.format(w=wildcard)
    file_loc_sql = ("select FILE_LOC, LANDSAT_PRODUCT_ID from ARD_L2_ALBERS_INVENTORY_V where"
                    " SATELLITE = :sat AND LANDSAT_PRODUCT_ID like :wildcard"
                    " order by LANDSAT_PRODUCT_ID desc")
    return select(connection, file_loc_sql, sat=sat, wildcard=wildcard)
