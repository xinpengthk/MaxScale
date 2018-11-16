#!/bin/bash

cd $(dirname $(realpath $0))

CONTAINER=maxscale-build

# See if we have a running build container
if [ "$(docker inspect $CONTAINER -f {{.State.Status}})" != "running" ]
then
    # No running build container, create a new one
    docker rm -vf $CONTAINER >& /dev/null
    docker run -d --name $CONTAINER --init -v $(realpath ../../):/src/ centos:7 sleep 31536000

    # Install build dependencies
    docker exec -i $CONTAINER bash <<EOF
yum -y install sudo
touch /etc/init.d/functions
/src/BUILD/install_build_deps.sh
EOF
fi

# Build packages inside the container
docker exec -i $CONTAINER bash <<EOF
mkdir -p /build && cd /build

# Configure if not configured
test ! -f /build/CMakeCache.txt && cmake /src/ -DCMAKE_BUILD_TYPE=Debug -DPACKAGE=Y -DTARGET_COMPONENT=all

# Build and copy packages
make && make package && rm -rf /packages/ && mkdir /packages/ && mv /build/*.rpm -t /packages/
EOF

# Copy packages from the container to the image build directory
docker cp $CONTAINER:/packages/ .
rm -f ./maxscale/*.rpm
mv ./packages/*.rpm -t ./maxscale/
rmdir packages

# Then build (or update) the image
docker-compose build maxscale_000

# Restart the MaxScale container with the new image
docker-compose up -d maxscale_000
