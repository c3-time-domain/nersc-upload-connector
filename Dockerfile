FROM rknop/devuan-chimaera-rknop
MAINTAINER Rob Knop <raknop@lbl.gov>

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y less python3 python3-pip apache2 libapache2-mod-wsgi-py3 libcap2-bin net-tools lynx \
    && apt-get clean \
    && rm -rf /var/apt/lists/*
RUN /sbin/setcap 'cap_net_bind_service=+ep' /usr/sbin/apache2

RUN pip3 install web.py

RUN mkdir /secrets
RUN echo "testing" >> /secrets/token
RUN mkdir /dest

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
RUN chown 95089:45703 /dest

COPY connector.py /var/www/html/

USER 95089:45703
RUN apachectl start

CMD [ "apachectl", "-D", "FOREGROUND", "-D", "APACHE_CONFDIR=/etc/apache2" ]
#CMD "/bin/bash"
