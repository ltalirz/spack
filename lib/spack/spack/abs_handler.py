# Copyright 2013-2023 Lawrence Livermore National Security, LLC and other
# Spack Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (Apache-2.0 OR MIT)

"""urllib handler for Azure Blob Storage (abs)"""

import urllib.parse
import urllib.request
import urllib.response


def _abs_open(url, method="GET"):
    """Open a reader stream to a blob object on Azure Blob Storage"""
    import spack.util.abs as abs_util

    url = urllib.parse.urlparse(req.get_full_url())
    absblob = abs_util.AzureBlob(url)
    #azure_url_sas = azureblob.azure_url_sas()
    #tty.debug("(read_from_url) azure_url_sas = %s" % (azure_url_sas))

    if not absblob.azure_blob_exists():
        raise URLError("Blob {0} does not exist on Azure blob storage".format(acsblob.get_container_blob_path()))
    
    stream = absblob.azure_get_blob_byte_stream()
    headers = absblob.azure_get_blob_headers()

    return urllib.response.addinfourl(stream, headers, url)

class UrllibAbsHandler(urllib.request.BaseHandler):
    def abs_open(self, req):
        return _abs_open(req)
