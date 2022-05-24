#!/usr/bin/env bash
#
#
. /etc/profile
APPNAME="update-objects-in-full-annot-pipeline"

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -jar lib/${APPNAME}.jar "$@" 2>&1
