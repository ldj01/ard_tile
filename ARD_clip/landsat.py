"""Functions for known data structures specific to landsat."""
import os
import re
import datetime


def match(product_id):
    """Extract common information from landsat product id.

    Args:
        product_id (str): landsat collection product id

    Returns:
        dict: group matches in product id

    Example:
        >>> match('LE07_L1TP_033037_20130902_20160907_01_A1')
        {'mission': 'LE07', 'wrspath': '033', ...}

    """
    return re.match(r'^(?P<mission>L[CET]\d{2})_\w+_(?P<wrspath>\d{3})'
                    r'(?P<wrsrow>\d{3})_(?P<acqdate>\d{8})_(?P<procdate>\d{8})'
                    r'_\d{2}_\w{2}$', product_id).groupdict()


def match_dt(product_id, fields=('acqdate', 'procdate'), dtfmt=r'%Y%m%d'):
    """Format results from match() as datetime."""
    return {
        k: (datetime.datetime.strptime(v, dtfmt) if k in fields else v)
        for k, v in match(product_id).items()
    }


def get_production_timestamp():
    """Get current UTC date/time in ISO 8601 format."""
    return datetime.datetime.utcnow().strftime(r'%Y-%m-%dT%H:%M:%SZ')


def generate_tile_id(product_id, current_tile, region, collection, version):
    """Create a current Landsat Tile ID based on a product ID.

    Args:
        product_id (str): landsat collection scene product id
        current_tile (dict): tile information including H/V for region
        region (str): shapefile grid region for provided H/V
        collection (int): ard landsat processing collection
        version (int): version of the collection processing this is

    Returns:
        str: landsat tile id

    Example:
        >>> generate_tile_id('LE07_L1TP_033037_20130902_20160907_01_A1',
        ...                  {'H': 11, 'V': 3}, 'CU', 1, 1)
        'LE07_CU_011003_20130902_20200101_C01_V01'

    """
    process_date = datetime.datetime.utcnow().strftime('%Y%m%d')
    product_info = match(product_id)
    options = dict(today=process_date, region=region, v=version, c=collection)
    options.update(current_tile)
    options.update(product_info)
    tile_id = ('{mission}_{region}_{H:03d}{V:03d}_{acqdate}_'
               '{today}_C{c:02d}_V{v:02d}')
    return tile_id.format(**options)


def read_metadatas(filename):
    """Read existing L2 metadata file as a big, long string."""
    if not os.path.isfile(filename):
        raise IOError('Could not find metadata file %s' % filename)
    return open(filename, 'r').read()
