# Program to update gene/strain/qtl/variant NAMEs and SYMBOLs in FULL_ANNOT table
#   which have the same RGD_ID with the ones in the GENES/STRAINS/QTLS/VARIANTS tables
#
. /etc/profile
APPNAME="update-objects-in-full-annot-pipeline"

APPDIR=/home/rgddata/pipelines/$APPNAME
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

ELIST=mtutaj@mcw.edu
if [ "$SERVER" == "REED" ]; then
    ELIST="$ELIST,rgd.pipelines@mcw.edu"
fi

cd $APPDIR
$APPDIR/_run.sh 

mailx -s "[$SERVER] Update Object Name Symbol in FULL_ANNOT table" $ELIST < $APPDIR/logs/summary.log
