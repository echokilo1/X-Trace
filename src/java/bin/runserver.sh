#!/bin/bash

#cpath="../../build/classes"
cpath="/var/lib/tomcat7/webapps/SimpleSrv/WEB-INF/classes"

TOMCAT_LIB=/usr/share/tomcat7/lib
for f in $TOMCAT_LIB/*; do
        cpath=$cpath:$f
done

LOCAL_LIB=../../WebContent/WEB-INF/lib
for f in $LOCAL_LIB/*; do
	cpath=$cpath:$f
done

java -Xmx512m -classpath $cpath simple.SimpleServer
