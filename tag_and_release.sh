#!/bin/bash
set -euf -o pipefail

create_tag () {
    if [[ -z "$1" ]]
      then
        VERSION="unknown"
      else
        VERSION=$1
    fi

    PARSL_VERSION=$(python3 -c "import parsl; print(parsl.__version__)")

    if [[ $PARSL_VERSION == "$VERSION" ]]
    then
        echo "Version requested matches package version: $VERSION"
    else
        echo "[ERROR] Version mismatch. User request: '$VERSION' while package version is: '$PARSL_VERSION'"
        exit 1
    fi

    echo "Creating tag"
    git tag -a "$VERSION" -m "Parsl $VERSION"

    echo "Pushing tag"
    git push origin --tags

}

update_version () {
    target_version=$VERSION
    echo "Target version = $target_version"
    cat << EOF > parsl/version.py
"""Set module version.

Year.Month.Day[alpha/beta/..]
Alphas will be numbered like this -> 2024.12.10a0
"""
VERSION = '$target_version'
EOF
}

"$@"
