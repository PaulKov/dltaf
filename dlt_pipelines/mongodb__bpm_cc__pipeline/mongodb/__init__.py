"""Compatibility wrapper for the private MongoDB BPM CC integration.

The canonical implementation lives in ``internal.dltaf_plugins.integrations``.
This package remains only to preserve temporary import compatibility while the
legacy ``dlt_pipelines`` package is being decommissioned.
"""

from internal.dltaf_plugins.integrations.mongodb_bpm_cc.mongodb import *  # noqa: F401,F403
