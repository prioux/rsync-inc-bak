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

VERSION="2.1"

# This function logs with a prefix similar in appearance to
# what rsync-incremental-backup.pl use.
function dolog {
  level="$1"
  message="$2"
  echo "$level: [$(date "+%Y-%m-%d %H:%M:%S")] $message"
}

dolog "INFO" "====================================================="
dolog "INFO" "============ Backup wrapper $VERSION starting ============"
dolog "INFO" "====================================================="

# Only one instance
export PATH=/sbin:$PATH  # pidof is often located there
PID=`pidof -x -o $$ -o %PPID $0`
if [ -n "$PID" ] ; then
  dolog "ERROR" "$0 already running."
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
dolog "INFO" "Backup list file is $BACKUP_LIST_FILE"



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
  dolog "ERROR" "Cannot find table of filesystems to backup '$BACKUP_LIST_FILE'"
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
      dolog "WARN" "Skipping backup '$name', because not matching filtering argument."
      continue # skip backups not matching arg #1
    fi
  fi
  dolog "INFO" "- - - - - - - - - - - - - - - - - - - - - - - - - -"
  eval $BACKUP_COM $BACKUP_RIB_OPTS $opts -n ${BACKUP_PREFIX}"${name}" "$dir" $BACKUP_DEST < /dev/null
done

