#!/bin/bash

#cpath="../../build/classes"
cpath="/h/ekrevat/gitdepot/X-Trace/src/java/bin/:/var/lib/tomcat7/webapps/SimpleSrv/WEB-INF/classes"

for f in `ls *.jar`; do
        cpath=$cpath:$f
done

JETTY_LIB=/usr/share/jetty/lib
for f in $JETTY_LIB/*; do
        cpath=$cpath:$f
done

TOMCAT_LIB=/usr/share/tomcat7/lib
for f in $TOMCAT_LIB/*; do
        cpath=$cpath:$f
done

LOCAL_LIB=/h/ekrevat/SimpleSrv/WebContent/WEB-INF/lib
for f in $LOCAL_LIB/*; do
	cpath=$cpath:$f
done

VELOCITY_LIB=/h/ekrevat/programs/velocity-1.7/lib
for f in $VELOCITY_LIB/*; do
	cpath=$cpath:$f
done

java -Xmx512m -Dlog4j.configuration=log4j.properties -Dxtrace.backend.webui.dir=/tmp/tracedata/webui -cp $cpath edu.berkeley.xtrace.server.XTraceServer /tmp/tracedata/