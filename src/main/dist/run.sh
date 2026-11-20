#!/usr/bin/env bash
#
# Program to update gene/strain/qtl/clinvar-variant/cell-line NAMEs and SYMBOLs in FULL_ANNOT table
#   to match the ones in the GENES/STRAINS/QTLS/GENOMIC_ELEMENTS tables
#
. /etc/profile
APPNAME="update-objects-in-full-annot-pipeline"

APPDIR=/home/rgddata/pipelines/$APPNAME
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

ELIST=mtutaj@mcw.edu
if [ "$SERVER" == "REED" ]; then
    ELIST="$ELIST rgd.pipelines@mcw.edu"
fi

cd $APPDIR || exit 1
$APPDIR/_run.sh
STATUS=$?

SUBJECT="[$SERVER] Update Object Name Symbol in FULL_ANNOT table"
if [ $STATUS -ne 0 ]; then
    SUBJECT="$SUBJECT -- FAILED (exit code $STATUS)"
fi
mailx -s "$SUBJECT" $ELIST < $APPDIR/logs/summary.log
exit $STATUS
