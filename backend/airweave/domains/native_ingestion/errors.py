"""Stable native admission failures without source content in diagnostics."""

from airweave.domains.entities.canonical.store import CanonicalStoreError


class NativeAdmissionError(CanonicalStoreError):
    """A native source, version or retained state cannot safely admit this write."""

    code = "native_admission_failed"


class NativeImportNotFound(NativeAdmissionError):
    """No import exists at the authenticated source/request identity."""

    code = "native_import_not_found"
