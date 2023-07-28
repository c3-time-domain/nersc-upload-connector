FROM rknop/devuan-chimaera-rknop
MAINTAINER Rob Knop <raknop@lbl.gov>

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get upgrade -y \
  && apt-get install -y less python3 python3-pytest python3-requests locales \
  && apt-get clean \
  && rm -rf /var/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ln -s /usr/bin/pytest-3 /usr/bin/pytest

# Generate UTF8 locale
RUN cat /etc/locale.gen | perl -pe 's/^# en_US.UTF-8/en_US.UTF-8/' > /etc/locale.gen_new \
    && mv /etc/locale.gen_new /etc/locale.gen \
    && locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN mkdir /storage
RUN chmod a+rwx /storage
RUN mkdir /storage_ro
RUN chmod a+rx /storage_ro

RUN mkdir /archive
WORKDIR /archive
