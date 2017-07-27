#!/bin/bash

# Pierre Rioux, November 2012
#
# This program reads a config file $HOME/cron-sys-backups.tab
# and invokes rsync-incremental-backup.pl for each backup
# defined in there.
#
# Improved April 2016
# The tab file can now be given as argument.
#
# Usage:
#
# - To read $HOME/cron-sys-backups.tab and perform all backups in there:
#
#   cron-sys-backup.sh
#
# - To read $HOME/cron-sys-backups.tab and perform a single backup 'abc':
#
#   cron-sys-backup.sh abc
#
# - To read myownlist.tab and perform all backups in there:
#
#   cron-sys-backup.sh -f myownlist.tab
#
# - To read myownlist.tab and perform and perform a single backup 'abc':
#
#   cron-sys-backup.sh -f myownlist.tab abc

VERSION="2.0"

echo "INFO: [`date +%H:%M:%S`] ==================================================="
echo "INFO: [`date +%H:%M:%S`] ====== Backup script $VERSION starting `date +%Y-%m-%d` ======"
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

# Tab file with lists of filesystems to backup
BACKUP_LIST_FILE="$HOME/cron-sys-backups.tab"
if test "X$1" == "X-f" -a -n "$2" ; then
  BACKUP_LIST_FILE="$2"
  shift; shift
fi
echo "INFO: [`date +%H:%M:%S`] Backup list file is $BACKUP_LIST_FILE"



# ================================================================
# === These options are reconfigurable in cron-sys-backups.tab ===
# ================================================================

# Options for rsync-incremental-backup.pl
BACKUP_RIB_OPTS="-F -P -K 14,1,9,17,25 -k 40"

# SSH user and path for system backups.
BACKUP_DEST="rdiffbak@macduff.cbrain.mcgill.ca:/srv/cbrainBackups/${SHOSTNAME}"

# Prefix for backups names.
BACKUP_PREFIX="${SHOSTNAME}_"

# ================================================================



# Verify config file exists
if ! test -r $BACKUP_LIST_FILE ; then
  echo "ERROR: [`date +%H:%M:%S`] Cannot find table of filesystems to backup '$BACKUP_LIST_FILE'"
  exit 2
fi

# Read config file backup table and backup each directory
#
# Lines that start with BACKUP_ are interpreted as variable assignements
# All other lines specify a backup to perform:
#   name path [options]
cat $BACKUP_LIST_FILE | perl -ne 'print unless /^\s*$|^\s*#/' | while builtin read name dir opts ; do
  if test "X${name:0:7}" == "XBACKUP_" ; then   # if line begins with BACKUP_ like a variable assignment
    eval "$name $dir $opts"
    continue
  fi
  if test $# -gt 0 ; then
    if test "X$name" != "X$1" -a "X$BACKUP_PREFIX$name" != "X$1" -a "X$dir" != "X$1" ; then
      echo "WARN: [`date +%H:%M:%S`] Skipping backup '$name', because not matching filtering argument."
      continue # skip backups not matching arg #1
    fi
  fi
  echo "INFO: [`date +%H:%M:%S`] - - - - - - - - - - - - - - - - - - - - - - - - - -"
  eval $BACKUP_COM $BACKUP_RIB_OPTS $opts -n ${BACKUP_PREFIX}"${name}" "$dir" $BACKUP_DEST < /dev/null
done

