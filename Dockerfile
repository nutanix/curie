FROM centos:7

RUN yum -y update && \
    yum install -y epel-release && \
    yum install -y \
      curl \
      gcc \
      gcc-c++ \
      git \
      ipmitool \
      libxml2-devel \
      libxslt-devel \
      libyaml-devel \
      make \
      openssh \
      openssh-clients \
      openssl \
      openssl-devel \
      python-devel \
      python-pip \
      qemu-img \
      sshpass \
      tar \
      unzip \
      wget 

RUN pip install --upgrade \
      pip \
      ansible==2.4.3 \
      pyvmomi \
      setuptools \
      sphinx \
      sphinx_rtd_theme

# Install protobuf.
WORKDIR /pb_tmp
ADD https://github.com/google/protobuf/releases/download/v3.1.0/protobuf-python-3.1.0.tar.gz .
RUN if [ -e protobuf-python-3.1.0.tar.gz ]; then tar -xzf protobuf-python-3.1.0.tar.gz; fi
ENV PROTO_PATH="/pb_tmp/protobuf-3.1.0"
WORKDIR /pb_tmp/protobuf-3.1.0
RUN ./configure && make -j4 && make install


# Install prerequisites.
WORKDIR /curie
ADD requirements.txt .
ADD requirements-dev.txt .
RUN pip install -r requirements-dev.txt

RUN yum install -y epel-release
RUN yum install -y krb5-workstation gssntlmssp
RUN yum install -y https://github.com/PowerShell/PowerShell/releases/download/v6.0.2/powershell-6.0.2-1.rhel.7.x86_64.rpm
