""" Methods for pre-defined database interactions """

import cx_Oracle

from util import logger


def connection(connstr):
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
    cursor.execute(sql, opts)
    logger.debug('Select SQL Statement: %s' % cursor.statement)
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
    cursor.execute(sql, opts)
    logger.debug('Commit SQL Statement: %s' % cursor.statement)
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
    cursor.executemany(None, array)
    logger.debug('Execute Many SQL Statement: %s' % cursor.statement)
    connection.commit()


def set_scene_to_inqueue(connection, scene_id):
    """ Set 'BLANK' to 'INQUEUE' processing status by matching scene_id """
    updatesql = "update ARD_PROCESSED_SCENES set PROCESSING_STATE = 'INQUEUE' where scene_id = :scene_id"
    update(connection, updatesql, scene_id=scene_id)


def reset_records(connection):
    """ reset any 'INWORK', 'INQUEUE' and 'ERROR' to 'BLANK' processing status """
    updatesql = ("update ARD_PROCESSED_SCENES set PROCESSING_STATE = 'BLANK', DATE_PROCESSED = sysdate "
                 "where PROCESSING_STATE in ('INWORK','INQUEUE','ERROR')")
    update(connection, updatesql)


def duplicate_scenes_insert(connection, dup_scene_list):
    """ insert duplicate scenes into ARD_PROCESSED_SCENES and mark as 'DUPLICATE' """
    dup_scenes_insert = ("insert /*+ ignore_row_on_dupkey_index(ARD_PROCESSED_SCENES, SCENE_ID_PK) */ "
                         "into ARD_PROCESSED_SCENES (scene_id,file_location, PROCESSING_STATE) values (:1,:2,:3)")
    if len(dup_scene_list) > 0:
        logger.info("Duplicate scenes inserted into ARD_PROCESSED_SCENES table: %s", dup_scene_list)
        update_many(connection, dup_scenes_insert, dup_scene_list)


def processed_scenes(connection, completed_scene_list):
    """ Insert processed segments with file location """
    processed_scenes_insert = ("insert /*+ ignore_row_on_dupkey_index(ARD_PROCESSED_SCENES, SCENE_ID_PK) */ "
                               "into ARD_PROCESSED_SCENES (scene_id,file_location) values (:1,:2)")
    update_many(connection, processed_scenes_insert, completed_scene_list)
