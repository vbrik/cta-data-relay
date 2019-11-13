FROM centos:7

# *Do* install man pages
RUN sed -i '/tsflags=nodocs/d' /etc/yum.conf
RUN rm /anaconda-post.log /root/anaconda-ks.cfg

RUN yum update -y
RUN yum install -y bash-completion-extras file iputils less \
                    man-db mlocate net-tools nmap psmisc screen socat \
                    tree vim wget htop

RUN yum install -y epel-release
RUN yum install -y python-ipython-console

RUN yum install -y yum-plugin-priorities # needed for OSG repos, apparently
RUN yum install -y https://repo.opensciencegrid.org/osg/3.4/osg-3.4-el7-release-latest.rpm
RUN yum install -y vo-client voms-clients-cpp \
                    osg-ca-certs cilogon-openid-ca-cert \
                    globus-gass-copy-progs \
                    gfal2-util gfal2-plugin-gridftp gfal2-plugin-file gfal2-python3

RUN pip3 install boto3 dnspython

RUN yum install -y zstd

RUN yum install -y git dstat

RUN updatedb

WORKDIR /app
ENTRYPOINT ["sleep", "inf"]
