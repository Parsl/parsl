import random
import re

import pytest

from parsl.utils import sanitize_dns_label_rfc1123, sanitize_dns_subdomain_rfc1123

# Ref: https://datatracker.ietf.org/doc/html/rfc1123
DNS_LABEL_REGEX = r'^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
DNS_SUBDOMAIN_REGEX = r'^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$'

test_labels = [
    "example-label-123",           # Valid label
    "EXAMPLE",                     # Case sensitivity
    "!@#example*",                 # Remove invalid characters
    "--leading-and-trailing--",    # Leading and trailing hyphens
    "..leading.and.trailing..",    # Leading and tailing dots
    "multiple..dots",              # Consecutive dots
    "valid--label",                # Consecutive hyphens
    "a" * random.randint(64, 70),  # Longer than 63 characters
    f"{'a' * 62}-a",               # Trailing hyphen at max length
]


def _generate_test_subdomains(num_subdomains: int):
    subdomains = []
    for _ in range(num_subdomains):
        num_labels = random.randint(1, 5)
        labels = [test_labels[random.randint(0, num_labels - 1)] for _ in range(num_labels)]
        subdomain = ".".join(labels)
        subdomains.append(subdomain)
    return subdomains


@pytest.mark.local
@pytest.mark.parametrize("raw_string", test_labels)
def test_sanitize_dns_label_rfc1123(raw_string: str):
    print(sanitize_dns_label_rfc1123(raw_string))
    assert re.match(DNS_LABEL_REGEX, sanitize_dns_label_rfc1123(raw_string))


@pytest.mark.local
@pytest.mark.parametrize("raw_string", ("", "-", "@", "$$$"))
def test_sanitize_dns_label_rfc1123_empty(raw_string: str):
    with pytest.raises(ValueError) as e_info:
        sanitize_dns_label_rfc1123(raw_string)
    assert str(e_info.value) == f"Sanitized DNS label is empty for input '{raw_string}'"


@pytest.mark.local
@pytest.mark.parametrize("raw_string", _generate_test_subdomains(10))
def test_sanitize_dns_subdomain_rfc1123(raw_string: str):
    assert re.match(DNS_SUBDOMAIN_REGEX, sanitize_dns_subdomain_rfc1123(raw_string))


@pytest.mark.local
@pytest.mark.parametrize("char", ("-", "."))
def test_sanitize_dns_subdomain_rfc1123_trailing_non_alphanumeric_at_max_length(char: str):
    raw_string = (f"{'a' * 61}." * 4) + f".aaaa{char}a"
    assert re.match(DNS_SUBDOMAIN_REGEX, sanitize_dns_subdomain_rfc1123(raw_string))


@pytest.mark.local
@pytest.mark.parametrize("raw_string", ("", ".", "..."))
def test_sanitize_dns_subdomain_rfc1123_empty(raw_string: str):
    with pytest.raises(ValueError) as e_info:
        sanitize_dns_subdomain_rfc1123(raw_string)
    assert str(e_info.value) == f"Sanitized DNS subdomain is empty for input '{raw_string}'"


@pytest.mark.local
@pytest.mark.parametrize(
    "raw_string", ("a" * 253, "a" * random.randint(254, 300)), ids=("254 chars", ">253 chars")
)
def test_sanitize_dns_subdomain_rfc1123_max_length(raw_string: str):
    assert len(sanitize_dns_subdomain_rfc1123(raw_string)) <= 253
