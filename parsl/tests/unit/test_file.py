import os
from unittest import mock

import pytest

from parsl import File

_MOCK_BASE = "parsl.data_provider.files."


@pytest.mark.local
@pytest.mark.parametrize("scheme", ("http", "https", "ftp", "ftps", "asdfasdf"))
def test_file_init_scheme(scheme):
    basename = "some_base_name"
    path = f"/some/path/1/2/3/{basename}"
    fqdn = "some.fqdn.example.com"
    exp_url = f"{scheme}://{fqdn}{path}"
    f = File(exp_url)
    assert f.url == exp_url, "Expected given url to be stored"
    assert f.scheme == scheme
    assert f.netloc == fqdn
    assert f.path == path
    assert f.filename == basename
    assert f.local_path is None, "Expect only set by API consumer, not constructor"


@pytest.mark.local
@pytest.mark.parametrize("url", ("some weird :// url", "", "a"))
def test_file_init_file_url_fallback(url):
    exp_url = "some weird :// url"
    f = File(exp_url)
    assert f.url == exp_url
    assert not f.netloc, "invalid host, should be no netloc"
    assert f.path == exp_url, "Should fail to fully parse, so path is whole url"
    assert f.filename == exp_url.rsplit("/", 1)[-1]

    assert f.scheme == "file"


@pytest.mark.local
def test_file_proxies_for_filepath(randomstring):
    # verify (current) expected internal hookup
    exp_filepath = randomstring()
    with mock.patch(
        f"{_MOCK_BASE}File.filepath", new_callable=mock.PropertyMock
    ) as mock_fpath:
        mock_fpath.return_value = exp_filepath
        f = File("")
        assert str(f) == exp_filepath
        assert os.fspath(f) == exp_filepath


@pytest.mark.local
@pytest.mark.parametrize("scheme", ("file://", ""))
def test_file_filepath_local_path_is_priority(scheme, randomstring):
    exp_path = "/some/local/path"
    url = f"{scheme}{exp_path}"
    f = File(url)

    f.local_path = randomstring()
    assert f.filepath == f.local_path

    f.local_path = None
    assert f.filepath == exp_path


@pytest.mark.local
def test_file_filepath_requires_local_accessible_path():
    with pytest.raises(ValueError) as pyt_exc:
        _ = File("http://").filepath

    assert "No local_path" in str(pyt_exc.value), "Expected reason in exception"


@pytest.mark.local
@pytest.mark.parametrize("scheme", ("https", "ftps", "", "file", "asdfasdf"))
def test_file_repr(scheme):
    netloc = "some.netloc"
    filename = "some_file_name"
    path = f"/some/path/{filename}"
    if scheme:
        url = f"{scheme}://{netloc}{path}"
    else:
        scheme = "file"
        url = path

    f = File(url)
    r = repr(f)
    assert r.startswith("<")
    assert r.endswith(">")
    assert f"<{type(f).__name__} " in r
    assert f" at 0x{id(f):x}" in r
    assert f" url={url}" in r
    assert f" scheme={scheme}" in r
    assert f" path={path}" in r
    assert f" filename={filename}" in r

    if scheme != "file":
        assert f" netloc={netloc}" in r
