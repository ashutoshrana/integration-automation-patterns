"""Release identity must fail closed on version or tag drift."""

import runpy
import sys
from pathlib import Path

import pytest


@pytest.mark.skipif(sys.version_info < (3, 11), reason="Release job uses Python 3.12 tomllib")
def test_release_version_guard():
    module = runpy.run_path(str(Path(__file__).parents[1] / "scripts" / "check_release.py"))
    check = module["validate_versions"]
    check("1.2.3", "1.2.3", "v1.2.3")
    for values in [
        ("1.2.3", "1.2.4", "v1.2.3"),
        ("1.2.3", "1.2.3", "v1.2.4"),
        ("1.2.3rc1", "1.2.3rc1", "v1.2.3rc1"),
        ("1.2.3", "1.2.3", "main"),
    ]:
        with pytest.raises(ValueError):
            check(*values)


@pytest.mark.skipif(sys.version_info < (3, 11), reason="Release job uses Python 3.12 tomllib")
def test_release_artifact_version_guard(tmp_path):
    import io
    import tarfile
    import zipfile

    module = runpy.run_path(str(Path(__file__).parents[1] / "scripts" / "check_release.py"))
    metadata = b"Name: sample-package\nVersion: 1.2.3\n"
    wheel_path = tmp_path / "sample.whl"
    with zipfile.ZipFile(wheel_path, "w") as wheel:
        wheel.writestr("sample_package-1.2.3.dist-info/METADATA", metadata)
    with tarfile.open(tmp_path / "sample.tar.gz", "w:gz") as source:
        info = tarfile.TarInfo("sample-1.2.3/PKG-INFO")
        info.size = len(metadata)
        source.addfile(info, io.BytesIO(metadata))
    module["check_artifacts"](tmp_path, "sample-package", "1.2.3")
    with pytest.raises(ValueError, match="does not match"):
        module["check_artifacts"](tmp_path, "sample-package", "1.2.4")
    with zipfile.ZipFile(wheel_path, "a") as wheel:
        wheel.writestr("sample/__pycache__/bad.pyc", b"generated")
    with pytest.raises(ValueError, match="bytecode"):
        module["check_artifacts"](tmp_path, "sample-package", "1.2.3")
