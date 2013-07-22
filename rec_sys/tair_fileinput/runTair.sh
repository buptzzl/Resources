#!/bin/sh
cd /dmp/lib64
NETLINK=libtbnet.so.0
SYSLINK=libtbsys.so.0
CLIENTLINK=libtairclientapi.so.0
NETSO=libtbnet.so.0.0.0
SYSSO=libtbsys.so.0.0.0
CLIENTSO=libtairclientapi.so.0.0.0

if [ -L $NETLINK ]
then :
else
ln -s $NETSO $NETLINK
fi
if [ -L $SYSLINK ]
then :
else
ln -s $SYSSO $SYSLINK
fi
if [ -L $CLIENTLINK ]
then :
else
ln -s $CLIENTSO $CLIENTLINK
fi
export LD_LIBRARY_PATH=/dmp/lib64
cd /dmp/bin
nohup ./tairFileImporter -d /inf/rtb/tmp/cookiematch  -m 122.49.4.135:5198 -s 122.49.4.136:5198 -g group_1 -c 40 >> /dmp/log/puttair.log 2>&1 &
