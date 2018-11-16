FROM mariadb:10.3

RUN apt-get update && apt-get -y install sudo
RUN useradd -m -s /bin/bash vagrant
RUN echo -n 'vagrant:vagrant' | chpasswd
RUN mkdir -p /etc/sudoers.d
RUN install -b -m 0440 /dev/null /etc/sudoers.d/vagrant
RUN echo 'vagrant ALL=NOPASSWD: ALL' >> /etc/sudoers.d/vagrant
RUN sed -i 's/Defaults    requiretty/#Defaults    requiretty/g' /etc/sudoers
RUN sed -i 's/mesg n/tty -s \&\& mesg n/' /root/.profile
