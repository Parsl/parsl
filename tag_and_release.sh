#!/bin/bash
set -euf -o pipefail

create_tag () {
    if [ -z "$1" ]
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

package() {

    rm -f dist/*

    echo "======================================================================="
    echo "Starting clean builds"
    echo "======================================================================="
    python3 setup.py sdist
    python3 setup.py bdist_wheel

    echo "======================================================================="
    echo "Done with builds"
    echo "======================================================================="


}

release () {
    echo "======================================================================="
    echo "Push to PyPi. This will require your username and password"
    echo "======================================================================="
    twine upload dist/*
}

"$@"