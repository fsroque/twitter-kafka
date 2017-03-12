FROM ubuntu:16.04
MAINTAINER Francisco Roque <francisco.roque@sonat.no>

######################################### base part

# keep upstart quiet - 1
RUN dpkg-divert --local --rename --add /sbin/initctl
RUN ln -sf /bin/true /sbin/initctl

# no tty
ENV DEBIAN_FRONTEND noninteractive

# get up to date
RUN apt-get update --fix-missing

# global installs [applies to all envs!]
RUN apt-get install -y build-essential git
RUN apt-get install -y python3 python3-dev python3-setuptools
RUN apt-get install -y python3-pip

# create a virtual environment and install all dependencies from pypi
RUN pip3 install virtualenv
RUN virtualenv -p /usr/bin/python3 /opt/venv
ADD requirements.txt /opt/venv/requirements.txt
RUN /opt/venv/bin/pip install -r /opt/venv/requirements.txt

###################################### deployment part

RUN mkdir -p /opt/app
COPY app /opt/app/

# Run the kafka sink and monitor for config.py changes
WORKDIR /opt/app
CMD /opt/venv/bin/python3 twitter_kafka_direct.py
