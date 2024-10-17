import pytest

from parsl.app.app import python_app
from parsl.data_provider.files import File


@python_app
def sort_strings(inputs=[], outputs=[]):
    with open(inputs[0].filepath, 'r') as u:
        strs = u.readlines()
        strs.sort()
        with open(outputs[0].filepath, 'w') as s:
            for e in strs:
                s.write(e)


@pytest.mark.cleannet
@pytest.mark.staging_required
def test_staging_ftp():
    """Test staging for an ftp file

    Create a remote input file (ftp) that points to file_test_cpt.txt.
    """

    unsorted_file = File('ftp://ftp.iana.org/pub/mirror/rirstats/arin/ARIN-STATS-FORMAT-CHANGE.txt')

    # Create a local file for output data
    sorted_file = File('sorted.txt')

    f = sort_strings(inputs=[unsorted_file], outputs=[sorted_file])
    f.result()
