#!/bin/bash

# Pierre Rioux, November 2012
#
# This program reads a config file $HOME/cron-sys-backups.tab
# and invokes rsync-incremental-backup.pl for each backup
# defined in there.

echo "INFO: [`date +%H:%M:%S`] ==================================================="
echo "INFO: [`date +%H:%M:%S`] ======== Backup script starting `date +%Y-%m-%d` ========"
echo "INFO: [`date +%H:%M:%S`] ==================================================="

# Only one instance
export PATH=/sbin:$PATH  # pidof is often located there
PID=`pidof -x -o $$ -o %PPID $0`
if [ -n "$PID" ] ; then
  echo "ERROR: [`date +%H:%M:%S`] $0 already running."
  exit 2
fi

# Short hostname
SHOSTNAME=`hostname -s`

# Backup command
BACKUP_COM="perl /usr/bin/rsync-incremental-backup.pl"



# =====================================================
# === Options configurable in cron-sys-backups.tab ===
# =====================================================

# Options for rsync-incremental-backup.pl
BACKUP_RIB_OPTS="-F -P -K 14,1,9,17,25 -k 40"

# SSH user and path for system backups.
BACKUP_DEST="rdiffbak@macduff.bic.mni.mcgill.ca:/srv/cbrainBackups/${SHOSTNAME}"

# Prefix for backups names.
BACKUP_PREFIX="${SHOSTNAME}_"

# =====================================================



# Read config file and configure.
if ! test -r $HOME/cron-sys-backups.tab ; then
  echo "ERROR: [`date +%H:%M:%S`] Cannot find $HOME/cron-sys-backups.tab"
  exit 2
fi

# There are three configurable variables: BACKUP_RIB_OPTS, BACKUP_DEST and BACKUP_PREFIX
cat $HOME/cron-sys-backups.tab | perl -ne 'print if /^\s*BACKUP_(RIB_OPTS|DEST|PREFIX)=/' > /tmp/csb.tmp.$$
source /tmp/csb.tmp.$$
rm -f /tmp/csb.tmp.$$

# Read config file backup table and backup each directory
cat $HOME/cron-sys-backups.tab | perl -ne 'print unless /^\s*$|^\s*#|^\s*BACKUP_[A-Z_]+=/' | while builtin read name dir opts ; do
  if test $# -gt 0 ; then
    if test "X$name" != "X$1" -a "X$BACKUP_PREFIX$name" != "X$1" -a "X$dir" != "X$1" ; then
      echo "WARN: [`date +%H:%M:%S`] Skipping backup '$name', because not matching filtering argument."
      continue # skip backups not matching arg #1
    fi
  fi
  echo "INFO: [`date +%H:%M:%S`] - - - - - - - - - - - - - - - - - - - - - - - - - -"
  eval $BACKUP_COM $BACKUP_RIB_OPTS $opts -n ${BACKUP_PREFIX}"${name}" "$dir" $BACKUP_DEST < /dev/null
done

