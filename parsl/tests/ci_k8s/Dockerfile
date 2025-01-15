FROM debian:bookworm

RUN apt-get update && apt-get upgrade -y

# git is needed for parsl to figure out it's own repo-specific
# version string, from the github-checked-out repo

# gcc and similar are needed for building taskvine

RUN apt-get update && apt-get install -y git procps python3 python3-dev python3-venv gcc build-essential make pkg-config mpich

RUN python3 -m venv /venv

ADD . /parsl
WORKDIR /
RUN git clone https://github.com/cooperative-computing-lab/cctools --shallow -b release/7.8.0

WORKDIR /cctools
RUN . /venv/bin/activate && apt install -y swig && ./configure --prefix=/ && make && make install

WORKDIR /parsl
RUN . /venv/bin/activate && pip3 install '.[kubernetes]' cloudpickle -r test-requirements.txt

