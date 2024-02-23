import warnings

import pytest

from parsl.providers import CobaltProvider


@pytest.mark.local
def test_deprecation_warning():

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        CobaltProvider()

        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)
        assert "CobaltProvider" in str(w[-1].message)
