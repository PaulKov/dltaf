"""Compatibility wrapper for the private uploader B057 integration.

The canonical implementation lives in ``internal.dltaf_plugins.integrations``.
This module remains only to preserve temporary import compatibility while the
legacy ``dlt_pipelines`` package is being decommissioned.
"""

from internal.dltaf_plugins.integrations.uploader_b057.source import *  # noqa: F401,F403

