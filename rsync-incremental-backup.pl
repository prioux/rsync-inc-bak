#!/usr/bin/perl -w

##############################################################################
#
#                                 rsync-incremental-backup.pl
#
# DESCRIPTION:
# Performs incremental backups using rsync.
#
##############################################################################

##############################################################################
#                                                                            #
#                       CONFIDENTIAL & PROPRIETARY                           #
#       Nothing herein is to be disclosed in any way without the prior       #
#           express written permission of Pierre Rioux                       #
#                                                                            #
#          Copyright 2008 Pierre Rioux, All rights reserved.                 #
#                                                                            #
##############################################################################
#
#    $Id$
#
#    $Log$

##########################
# Initialization section #
##########################

require 5.00;
use strict;
use vars qw( $VERSION $RCS_VERSION );
use IO::File;
use IO::Dir;
use POSIX ":sys_wait_h";
use File::Basename;

# Default umask
umask 027;

# Program's name and version number.
$RCS_VERSION='$Id: blahblah.pl,v 2.1 2012/09/06 13:00:00 prioux Exp $';
($VERSION) = ($RCS_VERSION =~ m#,v ([\w\.]+)#);
my ($BASENAME) = ($0 =~ /([^\/]+)$/);

# Get login name.
my $USER=getpwuid($<) || getlogin || die "Can't find USER from environment!\n";

#########
# Usage #
#########

sub Usage { # private
    print "This is $BASENAME $VERSION by Pierre Rioux.\n";
    print "\n";
    print "Usage: $BASENAME [-n name] [-k num] [-P] [-T] [-x rsyncoptions] source_dir incremental_root_dir\n";
    print "\n";
    print "Arguments:\n";
    print "\n";
    print "  source_dir           : can be any local or network directory specified using the SSH convention.\n";
    print "  incremental_root_dir : must be an existing local or network directory, initially empty.\n";
    print "\n";
    print "Options:\n";
    print "\n";
    print "  -k num               : keep the most recent 'num' incremental backups; delete older ones.\n";
    print "  -K rec,d1,d2,d3      : keep 'rec' most recent incremental backups; also keep backups.\n";
    print "                         that fall on month days d1 or d2 etc. Can be used with -k num too.\n";
    print "  -n name              : use 'name' as the backup name; default is basename of 'source_dir'.\n";
    print "  -x rsyncoptions      : 'rsyncoptions' are additional options for rsync, all as a single argument.\n";
    print "  -F                   : triggers --fake-super option on rsync destination.\n";
    print "  -P                   : create a pid file, 'name.pid'.\n";
    print "  -T                   : just do the backup, but do not generate the incremental tree.\n";
    exit 1;
}

##################################
# Global variables and constants #
##################################

my $DEBUG=0;               # -@
my $KEEPNUM=undef;         # -k
my $KEEPRECENT=undef       # -K
my $BAK_NAME=undef;        # -n
my $EXTRA_RSYNC_OPTS="";   # -x
my $WITH_PIDFILE=undef;    # -P
my $NO_TREE=undef;         # -T
my $FAKE_SUPER="";         # -F

my $RSYNC_OPTS="-a -x -E -H --delete-excluded --delete --stats --out-format=\"%o %9l %n %L\""; # NOTE: monitoring code looks for '-a -x -E'

# Current state
my $INTERACTIVE_IN  = (-t STDIN);
my $INTERACTIVE_OUT = (-t STDOUT);
my $ISINTERACTIVE   = ($INTERACTIVE_IN && $INTERACTIVE_OUT);

sub info;
sub MyDie;
sub MySystem;

##############################
# Parse command-line options #
##############################

my @ORIG_ARGS = @ARGV;
for (;@ARGV;) {
    # Add in the regex [] ALL single-character command-line options
    my ($opt,$arg) = ($ARGV[0] =~ /^-([\@kKnxPTF])(.*)$/);
    last if ! defined $opt;
    # Add in regex [] ONLY single-character options that
    # REQUIRE an argument, except for the '@' debug switch.
    if ($opt =~ /[kKnx]/ && $arg eq "") {
        if (@ARGV < 2) {
            print "Argument required for option \"$opt\".\n";
            exit 1;
        }
        shift;
        $arg=$ARGV[0];
    }
    $DEBUG=($arg ? $arg : 1)                     if $opt eq '@';
    $KEEPNUM=$arg                                if $opt eq 'k';
    $KEEPRECENT=$arg                             if $opt eq 'K';
    $BAK_NAME=$arg                               if $opt eq 'n';
    $EXTRA_RSYNC_OPTS=$arg                       if $opt eq 'x';
    $WITH_PIDFILE=1                              if $opt eq 'P';
    $NO_TREE=1                                   if $opt eq 'T';
    $FAKE_SUPER=1                                if $opt eq 'F';
    shift;
}

#################################
# Validate command-line options #
#################################

&Usage if @ARGV != 2;

my $SOURCE_FULL_SPEC      = shift;
my $INCREMENTAL_FULL_SPEC = shift;

# -----------------
# Check source
# -----------------

die "Invalid specification of source directory '$SOURCE_FULL_SPEC'.\n"
  if $SOURCE_FULL_SPEC !~ m/
       ^(?:
          (                         # start of $1
            (?:\w[\w\.\-]*\@)?      # user@
            \w[\w\.\-]*             # hostname
          )                         # $1 = user@hostname or hostname
          :                         # :
        )?                          # optional user@hostname:
        (\S+)                       # path, $2
       $/x;
my $SOURCE_USER_HOST=$1;
my $SOURCE_DIR=$2;

die "Specification of the source directory must not include a trailing '/'.\n"
  if $SOURCE_DIR =~ m#/$# && $SOURCE_DIR ne "/";

die "Invalid local source directory '$SOURCE_DIR'.\n"
  if ! $SOURCE_USER_HOST && (! -d $SOURCE_DIR || ! -r _);

# -----------------
# Check destination
# -----------------

die "Invalid specificaton for incremental root directory '$INCREMENTAL_FULL_SPEC'.\n"
  if $INCREMENTAL_FULL_SPEC !~ m/
       ^(?:
          (                         # start of $1
            (?:\w[\w\.\-]*\@)?      # user@
            \w[\w\.\-]*             # hostname
          )                         # $1 = user@hostname or hostname
          :                         # :
        )?                          # optional user@hostname:
        (\S+)                       # path, $2
       $/x;
my $INCREMENTAL_USER_HOST=$1;
my $INCREMENTAL_ROOT=$2;

die "Specification of the incremental root directory must not include a trailing '/'.\n"
  if $INCREMENTAL_ROOT =~ m#/$#;

die "Invalid local incremental root directory '$INCREMENTAL_ROOT'.\n"
  if ! $INCREMENTAL_USER_HOST && (! -d $INCREMENTAL_ROOT || ! -r _);

my $QUOTED_INCREMENTAL_ROOT = $INCREMENTAL_ROOT;
$QUOTED_INCREMENTAL_ROOT =~ s/'/'\\''/g;
$QUOTED_INCREMENTAL_ROOT = "'$QUOTED_INCREMENTAL_ROOT'";

if ($INCREMENTAL_USER_HOST) {
  my $fh = IO::File->new("ssh -x $INCREMENTAL_USER_HOST test -d $QUOTED_INCREMENTAL_ROOT '&&' echo OK-DIR 2>&1 |");
  my @ok = <$fh>;
  $fh->close();
  if (@ok != 1 || $ok[0] !~ /OK-DIR/) {
    die "Incremental root directory '$INCREMENTAL_ROOT' doesn't seem to exist on remote site.\n";
  }
}

# -----------------
# Other validations
# -----------------

die "Cannot have BOTH source and destination on remote servers!\n"
  if $SOURCE_USER_HOST && $INCREMENTAL_USER_HOST;
my $BACKUP_MODE = $SOURCE_USER_HOST ? "PULL" : $INCREMENTAL_USER_HOST ? "PUSH" : "LOCAL";

die "Value for -k option must be a number greater than 0.\n"
  if defined($KEEPNUM) && $KEEPNUM !~ /^[1-9]\d*$/;

die "Values for -K option should be in a format like '30,1' or '30,1,15' etc.\n"
  if defined($KEEPRECENT) && $KEEPRECENT !~ /^[1-9]\d*(,[1-9]\d*)+$/;

die "Name for backup option -n '$BAK_NAME' unacceptable, it should be a simple basename.\n"
  if defined($BAK_NAME) && $BAK_NAME !~ /^\w[\w\.\,\=\+\-\@\:]*$/;
$BAK_NAME ||= basename($SOURCE_DIR);
die "Specification of the source directory does not end with a simple basename.\n"
  if defined($BAK_NAME) && $BAK_NAME !~ /^\w[\w\.\,\=\+\-\@\:]*$/;

die "You cannot use the '--inplace' option with rsync, it would crush the incremental data!\n"
  if $EXTRA_RSYNC_OPTS =~ /--inplace/;

# Rsync options for fake super; requires destination to be mounted with user_xattr option.
if ($FAKE_SUPER) {
  $FAKE_SUPER = ($BACKUP_MODE eq "PUSH") ? '--rsync-path="rsync --fake-super"' : '--fake-super';
}

my @KEEPRECENT_MONTHDAYS  = ();
my $KEEPRECENT_MOSTRECENT = undef;
if (defined($KEEPRECENT)) {
  @KEEPRECENT_MONTHDAYS  = split(/\s*,\s*/,$KEEPRECENT); # "40,1,15" -> (40 , 1, 15) ; first number is most recent to keep; other are month days
  $KEEPRECENT_MOSTRECENT = shift @KEEPRECENT_MONTHDAYS;  # "40"
}

################
# Trap Signals #
################

sub SigCleanup { # private
     info "ERROR: Exiting: received signal \"" . $_[0] . "\".\n";
     exit 20;
}
$SIG{'INT'}  = \&SigCleanup;
$SIG{'TERM'} = \&SigCleanup;
$SIG{'HUP'}  = \&SigCleanup;
$SIG{'QUIT'} = \&SigCleanup;
$SIG{'PIPE'} = \&SigCleanup;
$SIG{'ALRM'} = \&SigCleanup;

###############################
#   M A I N   P R O G R A M   #
###############################

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
$year += 1900;  # Must ADD it! Always! See the doc!
$mon  += 1;     # From 0..11 to 1..12
my $pday = ('Sun','Mon','Tue','Wed','Thu','Fri','Sat')[$wday];
my $pmon = ('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')[$mon-1];
my $timestamp = sprintf("%4d-%2.2d-%2.2dT%2.2d%2.2d%2.2d",$year,$mon,$mday,$hour,$min,$sec);

info sprintf("%s %s starting on %s %d %s %4d at %2.2dh %2.2dm %2.2ds\n",$BASENAME,$VERSION,$pday,$mday,$pmon,$year,$hour,$min,$sec);
info "Process ID   : $$\n";
info "Timestamp    : $timestamp\n";
info "Arguments    : " . join(" ",@ORIG_ARGS) . "\n";

my $INC_BASE  = "$BAK_NAME.$timestamp";                   #        name.timestamp
my $LOG_BASE  = "$INC_BASE.rsync_log";                    #        name.timestamp.rsync_log

# Create PID file if -P option supplied
if ($WITH_PIDFILE) {
    my $host = `hostname`; chomp $host;
    if ($BACKUP_MODE ne "PUSH") {
        $WITH_PIDFILE = "$INCREMENTAL_ROOT/$BAK_NAME.rsync_inc.pid";
        my $fh = new IO::File(">$WITH_PIDFILE") or MyDie("Can't create PID file '$WITH_PIDFILE': $!");
        print $fh "$$\@$host\n";
        $fh->close();
    } else {
        $WITH_PIDFILE = "$QUOTED_INCREMENTAL_ROOT/$BAK_NAME.rsync_inc.pid";
        MySystem("ssh -x $INCREMENTAL_USER_HOST \"echo '$$\@$host' > $WITH_PIDFILE\"");
    }
}

# Scan output directory for previous backups
my @inc_entries = ();
if ($BACKUP_MODE eq "PUSH") {
    my $fh = IO::File->new("ssh -x $INCREMENTAL_USER_HOST /bin/ls -1 $QUOTED_INCREMENTAL_ROOT|");
    @inc_entries = <$fh>;
    $fh->close();
    chomp @inc_entries;
} else { # PULL or LOCAL
    my $dh = IO::Dir->new($INCREMENTAL_ROOT) or MyDie "Can't read directory '$INCREMENTAL_ROOT': $!";
    @inc_entries = $dh->read;
    $dh->close();
}
@inc_entries = grep(/^\Q$BAK_NAME\E\.\d\d\d\d-\d\d-\d\dT\d\d:?\d\d:?\d\d$/, @inc_entries); # note: an older version had ':' in names
@inc_entries = sort { $a cmp $b } @inc_entries;

# Note: the most recent tree with the timestamp is always just like the main storage in $BAK_NAME
# so it's not really an incremental backup.
info "There is a total of " . (scalar(@inc_entries)) . " backups already present.";

# Clean up output directory of old backups
my $time_to_erase = 0;
if (defined($KEEPNUM) || defined($KEEPRECENT)) {
  my ($to_erase,$to_keep) = &IdentifyOldBackups( \ @inc_entries ); # use $KEEPNUM and/or $KEEPRECENT
  if (@$to_erase > 0) {
      info "There are " . scalar(@$to_erase) . " old incremental backups to erase.";
  }
  my $erase_start = time;
  while (@$to_erase > 0) {
    my $old_erase = shift(@$to_erase);
    info "Erasing old incremental tree entry '$old_erase'.";
    if ($BACKUP_MODE eq "PUSH") {
        MySystem("ssh -x $INCREMENTAL_USER_HOST /bin/rm -rf $QUOTED_INCREMENTAL_ROOT/$old_erase $QUOTED_INCREMENTAL_ROOT/$old_erase.rsync_log");
    } else {
        MySystem("/bin/rm","-rf", "$INCREMENTAL_ROOT/$old_erase", "$INCREMENTAL_ROOT/$old_erase.rsync_log");
    }
  }
  $time_to_erase = time - $erase_start;
  if ($time_to_erase > 2) {
      info "Time to erase backups: $time_to_erase seconds.";
  } else {
      $time_to_erase = 0;
  }
}

my $starttime = time;

info "Performing rsync backups...\n";
info "Source       : $SOURCE_FULL_SPEC\n";
info "Work Area    : $INCREMENTAL_FULL_SPEC/$BAK_NAME\n";
info "Incremental  : $INCREMENTAL_FULL_SPEC/$INC_BASE\n";
info "Log file     : $INCREMENTAL_FULL_SPEC/$LOG_BASE\n";

# Child code; performs the backup
my $LOCAL_LOG = $BACKUP_MODE eq "PUSH" ? "/tmp/$LOG_BASE.$$" : "$INCREMENTAL_ROOT/$LOG_BASE";
my $childpid = fork;
if (!$childpid) {
    $WITH_PIDFILE=undef; # don't want to trigger END block here
    if ($BACKUP_MODE eq "PUSH") {
      my $source_dir_slash = $SOURCE_DIR eq "/" ? "/" : "$SOURCE_DIR/"; # prevents '//'
      MySystem("rsync $RSYNC_OPTS $FAKE_SUPER $EXTRA_RSYNC_OPTS $source_dir_slash $INCREMENTAL_USER_HOST:$QUOTED_INCREMENTAL_ROOT/$BAK_NAME > $LOCAL_LOG 2>&1");  # the slash is IMPORTANT after source
      MySystem("scp -q $LOCAL_LOG $INCREMENTAL_USER_HOST:$QUOTED_INCREMENTAL_ROOT/$LOG_BASE");
    } else {
      my $source_full_spec_slash = $SOURCE_FULL_SPEC =~ m#:/$# ? $SOURCE_FULL_SPEC : "$SOURCE_FULL_SPEC/"; # prevents '//'
      MySystem("rsync $RSYNC_OPTS $EXTRA_RSYNC_OPTS $source_full_spec_slash $QUOTED_INCREMENTAL_ROOT/$BAK_NAME > $QUOTED_INCREMENTAL_ROOT/$LOG_BASE 2>&1");  # the slash is IMPORTANT after source
    }
    exit 0;
}

# Parent code; monitor the progress
info "PID of rsync : $childpid\n";
info "Waiting for backup to finish...\n";
if (! $INTERACTIVE_OUT) {
    wait;
} else {
    my $rsyncstarted = 0;
    for (my $cnt=0;;$cnt++) {
        sleep 5;
        my $done = waitpid($childpid,WNOHANG);
        my $finished=($done == $childpid || $done == -1);
        if (!$rsyncstarted) {
            if (!$finished) {
                my @com = `ps auxww`;
                @com = grep(!/sudo.*rsync|\/bin\/bash/,grep(/rsync -a -x -E/,@com));
                next unless @com;
            }
            $rsyncstarted = 1;
            if ($BACKUP_MODE ne "PUSH") {
                my $fh = new IO::File "df -h $INCREMENTAL_ROOT | head -1|"
                    or MyDie "Cannot open pipe to 'df': $!\n";
                info "STAT: ",<$fh>;
                $fh->close();
            }
            $cnt=0; # override
            # fall through
        }
        next unless $finished || ($cnt % 12 == 0); # we print our progress report every minute
        if ($BACKUP_MODE ne "PUSH") {
            my $fh = new IO::File "df -h $INCREMENTAL_ROOT | tail -n +2|"
                or MyDie "Cannot open pipe to 'df': $!\n";
            info "STAT: ",<$fh>;
            $fh->close();
        }
        last if $finished;
    }
}

info "Finished rsync backup in ",(time-$starttime), " seconds.\n";

# Check rsync output file
my $fh = new IO::File "tail -10 $LOCAL_LOG|";
my $rsynctail = join("",<$fh>);
$fh->close();
unlink($LOCAL_LOG) if $BACKUP_MODE eq "PUSH";
if ($rsynctail !~ /^total size is/m ) {
    info "ERROR: rsync did not seem to complete successfully. Check log.";
    exit 10;
}

# Check destination directory
my $dest_exists = 0;
if ($BACKUP_MODE ne "PUSH") {
   $dest_exists = -d "$INCREMENTAL_ROOT/$BAK_NAME";
} else {
   my $fh = IO::File->new("ssh -x $INCREMENTAL_USER_HOST test -d $QUOTED_INCREMENTAL_ROOT/$BAK_NAME '&&' echo OK-DIR 2>&1 |");
   my @ok = <$fh>;
   $fh->close();
   $dest_exists = (@ok == 1 && $ok[0] =~ /OK-DIR/);
}
if (! $dest_exists) {
    info "ERROR: Destination copy was not created?!?";
    info "Exiting.";
    exit 10;
}

if ($NO_TREE) {
    info "No incremental hardlink tree required for current snapshot '$INC_BASE'.\n"
} else {
    # Create incremental tree
    my $treestarttime = time;
    info "Making hardlink tree for current snapshot '$INC_BASE' ...";
    if ($BACKUP_MODE eq "PUSH") {
        MySystem("ssh -x $INCREMENTAL_USER_HOST \"cd $QUOTED_INCREMENTAL_ROOT/$BAK_NAME ; find . -print | cpio -dplm ../$INC_BASE 2> /dev/null\"");
    } else { # PULL or LOCAL
        chdir("$INCREMENTAL_ROOT/$BAK_NAME") || MyDie "Can't cd to srcbase ?!?";
        MySystem("find . -print | cpio -dplm ../$INC_BASE 2> /dev/null");
    }
    info "Incremental tree constructed in ", (time-$treestarttime), " seconds.\n";
    # IMPORTANT NOTE! At this point the CWD for this process has changed!
}

info "Total time for rsync backup and tree generation: ",(time-$starttime), " seconds.\n";
info "Total time for erasing, backup and tree generation: ",((time-$starttime)+$time_to_erase), " seconds.\n" if $time_to_erase > 0;
info "All done. Exiting.\n";
exit 0;

#############################
#   S U B R O U T I N E S   #
#############################

# General STDOUT logging mechanism. By default, prefix all lines with "INFO: ".
sub info {
    my $messages = join("",@_);
    my $prefix = "INFO: ";
    $prefix = "$1: " if $messages =~ s/^([A-Z]+):\s*//;
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)
       = localtime(time);
    $year += 1900;  # Must ADD it! Always! See the doc!
    #my $stamp = sprintf("%4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d",$year,$mon,$mday,$hour,$min,$sec);
    my $stamp = sprintf("%2.2d:%2.2d:%2.2d",$hour,$min,$sec);
    my @splitm = split(/\n/,$messages);
    foreach my $line (@splitm) {
       $line =~ s/^\s*//;
       $line =~ s/\s*$/\n/;
       print $prefix,"[$stamp] ",$line;
    }
}

sub MyDie {
    my $message = shift;
    info "FATAL: $message";
    exit 0;
}

sub MySystem {
    my @args = @_;
    if ($DEBUG) {
        info "DEBUG: system('",join(" ",@args),"')\n";
    }
    my $ret = system(@args);
    $ret;
}

sub IdentifyOldBackups {
    my $entries = shift; # [ "bak_name.timestamp", "bak_name.timestamp" ... ]

    my @inc_entries    = sort { $a cmp $b } @$entries; # just to be sure they're sorted
    my $todelete       = [];
    my %keep_monthdays = map { ($_+0) => 1 } @KEEPRECENT_MONTHDAYS;

    if (defined($KEEPRECENT_MOSTRECENT) && $KEEPRECENT_MOSTRECENT > 0 && @inc_entries > $KEEPRECENT_MOSTRECENT) {
      for (my $i=@inc_entries-1-$KEEPRECENT_MOSTRECENT;$i >=0;$i--) {
        my $name = $inc_entries[$i];
        next unless $name =~ /(\d\d\d\d)-(\d\d)-(\d\d)T/;
        my $md = $3+0;
        next if $keep_monthdays{$md};
        my $todel = splice(@inc_entries,$i,1);
        unshift(@$todelete, $todel);
      }
    }

    if (defined($KEEPNUM) && @inc_entries > $KEEPNUM) {
        my $toomany = @inc_entries - $KEEPNUM;
        my @toomany = splice(@inc_entries,0,$toomany);
        unshift(@$todelete, @toomany);
    }

    return($todelete, \ @inc_entries);
}

END {
    if (defined($BACKUP_MODE) && defined($WITH_PIDFILE) && $WITH_PIDFILE ne "1") {
        if ($BACKUP_MODE ne "PUSH") {
            unlink $WITH_PIDFILE if (defined($WITH_PIDFILE) && $WITH_PIDFILE ne "1");
        } else {
            MySystem("ssh -x $INCREMENTAL_USER_HOST /bin/rm -f $WITH_PIDFILE");
        }
    }
}

