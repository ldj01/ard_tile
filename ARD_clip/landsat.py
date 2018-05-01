""" Functions for known data structures specific to landsat """

import re
import datetime


def match(product_id):
    """ Extracts common information from landsat product id

    Args:
        product_id (str): landsat collection product id

    Returns:
        dict: group matches in product id

    Example:
        >>> match('LE07_L1TP_033037_20130902_20160907_01_A1')
        {'mission': 'LE07', 'wrspath': '033', ...}
    """
    return re.match(r'^(?P<mission>L[CET]\d{2})_\w+_(?P<wrspath>\d{3})(?P<wrsrow>\d{3})_'
                    r'(?P<acqdate>\d{8})_(?P<procdate>\d{8})_\d{2}_\w{2}$', product_id).groupdict()


def match_dt(product_id, fields=('acqdate', 'procdate'), dtfmt=r'%Y%m%d'):
    """ Returns results from match(), formatted as datetime """
    return {k: ( datetime.datetime.strptime(v, dtfmt) if k in fields else v )
            for k, v in match(product_id).items()}
