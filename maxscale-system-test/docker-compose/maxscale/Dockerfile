FROM centos:7

RUN yum -y install sudo
RUN useradd -m -s /bin/bash vagrant
RUN echo -n 'vagrant:vagrant' | chpasswd
RUN mkdir -p /etc/sudoers.d
RUN install -b -m 0440 /dev/null /etc/sudoers.d/vagrant
RUN echo 'vagrant ALL=NOPASSWD: ALL' >> /etc/sudoers.d/vagrant
RUN sed -i 's/Defaults    requiretty/#Defaults    requiretty/g' /etc/sudoers
COPY maxscale*.rpm /
RUN yum -y install /maxscale*.rpm
CMD ["maxscale", "-d", "-Umaxscale", "-lstdout"]
