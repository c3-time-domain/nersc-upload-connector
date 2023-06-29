FROM rknop/devuan-chimaera-rknop
MAINTAINER Rob Knop <raknop@lbl.gov>

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get upgrade -y \
  && apt-get install -y less python3 python3-pytest net-tools locales \
  && apt-get clean \
  && rm -rf /var/apt/lists/*

# Generate UTF8 locale
RUN cat /etc/locale.gen | perl -pe 's/^# en_US.UTF-8/en_US.UTF-8/' > /etc/locale.gen_new \
    && mv /etc/locale.gen_new /etc/locale.gen \
    && locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN mkdir /archive
WORKDIR /archive
