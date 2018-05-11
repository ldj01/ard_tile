""" Interactions with external system resources on network """

import urllib2

from util import logger


def stage_files(file_locations, soap_envelope, ignore_errors=True):
    """ Request required L2 scene bundles be moved to fastest cache available. """
    try:
       logger.info('Start staging...')
       url="https://dds.cr.usgs.gov/HSMServices/HSMServices?wsdl"
       headers = {'content-type': 'text/xml'}

       files = ''.join(['<files>{}</files>'.format(x) for x in file_locations])
       soap_envelope = soap_envelope.format(FILES_PLACE_HOLDER=files, WAIT="false")

       logger.debug('SOAP Envelope: {0}'.format(soap_envelope))
       request_object = urllib2.Request(url, soap_envelope, headers)

       response = urllib2.urlopen(request_object)

       html_string = response.read()
       logger.info('Stage response: {0}'.format(html_string))

    except Exception as e:
        if not ignore_errors:
            logger.exception('Error staging files!')
            raise
        else:
            logger.warning('Error staging files: {0}.  Continue anyway.'.format(e))

    else:
       logger.info('Staging succeeded...')
