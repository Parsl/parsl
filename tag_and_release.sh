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

update_version () {
    latest=$(pip index versions parsl | grep LATEST | awk '{print $2}')
    echo "Latest version = $latest"
    target_version=$(date +%Y.%m.%d)
    echo "Target version = $target_version"
    if [[ $latest == $target_version ]]
    then
       echo "Conflict detected. Target version already is uploaded on Pypi"
       exit -1
    else
        cat << EOF > parsl/version.py
"""Set module version.

<Major>.<Minor>.<maintenance>[alpha/beta/..]
Alphas will be numbered like this -> 0.4.0a0
"""
VERSION = '$target_version'
EOF
    fi

}

"$@"
