FROM debian:trixie

RUN apt-get update && apt-get upgrade -y

RUN apt-get update && apt-get install -y sudo openssh-server

RUN apt-get update && apt-get install -y curl less vim

# git is needed for parsl to figure out it's own repo-specific
# version string
RUN apt-get update && apt-get install -y git

# useful stuff to have around
RUN apt-get update && apt-get install -y procps

# for building documentation
RUN apt-get update && apt-get install -y pandoc

# for monitoring visualization
RUN apt-get update && apt-get install -y graphviz wget

# for commandline access to monitoring database
RUN apt-get update && apt-get install -y sqlite3

RUN apt-get update && apt-get install -y python3.12 python3.12-dev
RUN apt-get update && apt-get install -y python3.12-venv

RUN apt-get update && apt-get install -y gcc build-essential make pkg-config mpich

RUN python3.12 -m venv /venv

ADD . /src
WORKDIR /src

RUN . /venv/bin/activate && pip3 install '.[kubernetes]' -r test-requirements.txt
