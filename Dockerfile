# Trixie doesn't work because it goes to python 3.13, which removes cgi, which web.py depends on
# TODO: rewrite this all to use flask.  web.py is no longer maintained, it seems.
FROM debian:bookworm-20251229 AS base
MAINTAINER Rob Knop <raknop@lbl.gov>

# These next two are what's needed to run as raknop on NERSC.
# If somebody else is isntalling this, they will need to specify
#   a different UID/GID.  (It's possible they can do this all at runtime
#   with an init-container or something like that.)
ARG UID=95089
ARG GID=45703

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y less python3 python3-venv apache2 libapache2-mod-wsgi-py3 \
       libcap2-bin net-tools netcat-openbsd lynx patch \
    && apt-get clean \
    && rm -rf /var/apt/lists/*

# ======================================================================
# apt-getting pip installs a full dev environment, which we don't
#   want in our final image.  (400 unnecessary MB.)

FROM base AS build

RUN apt-get update && apt-get install -y python3-pip

RUN mkdir /venv
RUN python3 -mvenv /venv

RUN source /venv/bin/activate \
  && pip install web.py

# ======================================================================

FROM base AS final

COPY --from=build /venv/ /venv/
ENV PATH=/venv/bin:$PATH

# This needs to get replaced with a bind mound at runtime
RUN mkdir /secrets
RUN echo "testing testing" >> /secrets/connector_tokens
RUN mkdir /dest

RUN /sbin/setcap 'cap_net_bind_service=+ep' /usr/sbin/apache2

RUN ln -s ../mods-available/socache_shmcb.load /etc/apache2/mods-enabled/socache_shmcb.load
RUN ln -s ../mods-available/ssl.load /etc/apache2/mods-enabled/ssl.load
RUN ln -s ../mods-available/ssl.conf /etc/apache2/mods-enabled/ssl.conf
RUN ln -s ../mods-available/rewrite.load /etc/apache2/mods-enabled/rewrite.load
RUN rm /etc/apache2/sites-enabled/000-default.conf
RUN echo "Listen 8080" > /etc/apache2/ports.conf
COPY connector.conf /etc/apache2/sites-available/
RUN ln -s ../sites-available/connector.conf /etc/apache2/sites-enabled/connector.conf

# Patches
RUN mkdir patches
COPY ./patches/* patches/
RUN patch -p1 /etc/apache2/mods-available/mpm_event.conf < ./patches/mpm_event.conf_patch
RUN rm -rf patches

# Do scary permissions stuff since we'll have to run
#  as a normal user.  But, given that we're running as
#  a normal user, that makes this less scary.
RUN mkdir -p /var/run/apache2
RUN chmod a+rwx /var/run/apache2
RUN mkdir -p /var/lock/apache2
RUN chmod a+rwx /var/lock/apache2
RUN chmod -R a+rx /etc/ssl/private
RUN mkdir -p /var/log/apache2
RUN chmod -R a+rwx /var/log/apache2
RUN chown $UID:$GID /dest

COPY connector.py /var/www/html/

USER $UID:$GID
RUN apachectl start

CMD [ "apachectl", "-D", "FOREGROUND", "-D", "APACHE_CONFDIR=/etc/apache2" ]
#CMD "/bin/bash"
