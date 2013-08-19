#!/usr/bin/perl -w

##############################################################################
#
#                                 rsync_inc_stats.pl
#
# DESCRIPTION:
# Dumps stats from incremental backups.
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
use Data::Dumper;
use Date::Manip;

# Default umask
umask 027;

# Program's name and version number.
$RCS_VERSION='$Id: blahblah.pl,v 1.0 2007/11/08 18:40:36 prioux Exp $';
($VERSION) = ($RCS_VERSION =~ m#,v ([\w\.]+)#);
my ($BASENAME) = ($0 =~ /([^\/]+)$/);

# Get login name.
my $USER=getpwuid($<) || getlogin || die "Can't find USER from environment!\n";

#########
# Usage #
#########

sub Usage { # private
    print <<USAGE;
$BASENAME $VERSION

Usage: $BASENAME [options] inc_base [inc_base...]

Options:

  * Select incremental backup location:
       [-D dir]          Directory of all incremental backups

  * Select reports:
       [-f]              Number of files in incremental backups
       [-s]              Amount of data in incremental backups
       [-F]              Total number of files
       [-S]              Total amount of data 
       [-a]              Same as all reports: -F -f -S -s

  * Filter reports:
       [-A afterdate]    Only Report backups AFTER afterdate
       [-B beforedate]   Only Report backups BEFORE beforedate
       [-T]              Only report top backup for each name

  * Display options:
       [-N numentries]   How many entries to report (default: one terminal's page)

About dates: They can be specified as:
   2013        whole year
   2013-04     year + month
   2013-04-17  specific date
   -3          three days before today

About the arguments:
   The inc_base arguments are subpath to entire sets of incremental backups.
   They should point to existing subdirectories under the -D dir option.
   For instance, "amnesia/amnesia_root" or "netmind_vms/demo/demo_usr_local" etc.

USAGE
    exit 1;
}

##################################
# Global variables and constants #
##################################

my $DEBUG=0;
my $ALL_BACKUPS_BASEDIR=".";
my @REPORT_ORDER=();
my $NUM_ENTRIES=30;
my $AFTER_DATE="";
my $BEFORE_DATE="";
my $TOP_BASE_ONLY=0;

my $SEP="\t"; # not used anymore, was for CSV dump

# Get number of rows
if (-t STDIN) {
  my @stty = `stty -a | head -1`;
  my ($rows) = $stty[0] =~ m#row(?:s?)\s+(\d+)#;
     ($rows) = $stty[0] =~ m#(\d+)\s+row# if !defined $rows;
  $NUM_ENTRIES=$rows-3 if defined($rows) && $rows > 5;
}


##############################
# Parse command-line options #
##############################

for (;@ARGV;) {
    # Add in the regex [] ALL single-character command-line options
    my ($opt,$arg) = ($ARGV[0] =~ /^-([\@DCfsFSNaBAT])(.*)$/);
    last if ! defined $opt;
    # Add in regex [] ONLY single-character options that
    # REQUIRE an argument, except for the '@' debug switch.
    if ($opt =~ /[DNCBA]/ && $arg eq "") {
        if (@ARGV < 2) {
            print "Argument required for option \"$opt\".\n";
            exit 1;
        }
        shift;
        $arg=$ARGV[0];
    }
    $DEBUG=($arg ? $arg : 1)                     if $opt eq '@';
    $ALL_BACKUPS_BASEDIR=$arg                    if $opt eq 'D';
    push(@REPORT_ORDER,'incf')                   if $opt eq 'f';
    push(@REPORT_ORDER,'totf')                   if $opt eq 'F';
    push(@REPORT_ORDER,'incs')                   if $opt eq 's';
    push(@REPORT_ORDER,'tots')                   if $opt eq 'S';
    @REPORT_ORDER=qw( totf incf tots incs )      if $opt eq 'a';
    $SEP=$arg                                    if $opt eq 'C'; # not used anymore
    $BEFORE_DATE=$arg                            if $opt eq 'B';
    $AFTER_DATE=$arg                             if $opt eq 'A';
    $NUM_ENTRIES=$arg                            if $opt eq 'N';
    $TOP_BASE_ONLY=1                             if $opt eq 'T';
    shift;
}

#################################
# Validate command-line options #
#################################

&Usage if @ARGV == 0;
our @BASES = @ARGV; # and they belong to you.

# Default when no option: -s
@REPORT_ORDER=('incs') if @REPORT_ORDER == 0;

$AFTER_DATE  = &ValidateDate($AFTER_DATE)  if $AFTER_DATE;
$BEFORE_DATE = &ValidateDate($BEFORE_DATE) if $BEFORE_DATE;

################
# Trap Signals #
################

sub SigCleanup { # private
     die "\nExiting: received signal \"" . $_[0] . "\".\n";
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

our $STATS={};
our @UNIQ_BASES=();
our @UNIQ_DATES=();
our @ALL_KEYS=();
our %DIRECT_REPORT=();

chdir($ALL_BACKUPS_BASEDIR) or die "Can't CD to '$ALL_BACKUPS_BASEDIR': $!\n";

foreach my $base (@BASES) {
    &GatherStats($base);
}

&DumpReport;

exit 0;

#############################
#   S U B R O U T I N E S   #
#############################

sub GatherStats {
    my $base = shift;
    my @rsyncout_files = < $base*.rsync_log >;
    die "Can't find rsync inc report with base '$base'.\n" unless @rsyncout_files > 0;

    my $desc_base = $base;    # blah/mindstorm_etc
    $desc_base =~ s#.*/##;

    my $statsbase = $STATS->{$desc_base} ||= {};

    foreach my $rsyncout_file (@rsyncout_files) {
        my $fh  = IO::File->new("tail -40 $rsyncout_file|") or die "Can't open pipe to tail: $!\n";
        my $out = join("",<$fh>);
        $fh->close();

        # Number of files: 6287
        # Number of files transferred: 91
        # Total file size: 1031860036 bytes
        # Total transferred file size: 10939485 bytes

        my ($totf) = ($out =~ /Number of files: (\d+)/);
        my ($incf) = ($out =~ /Number of files transferred: (\d+)/);
        my ($tots) = ($out =~ /Total file size: (\d+)/);
        my ($incs) = ($out =~ /Total transferred file size: (\d+)/);

        die "Can't parse rsync out:\n$out\n"
          if ! defined($totf) || ! defined($incf) || ! defined($tots) || ! defined($incs);

        # mindstorm_root.2013-08-06T052612.rsync_log
        my ($tstamp) = ($rsyncout_file =~ /(\d\d\d\d-\d\d-\d\d)T\d\d:?\d\d:?\d\d/);
        $statsbase->{$tstamp} = {
          'totf' => $totf,
          'incf' => $incf,
          'tots' => $tots,
          'incs' => $incs,
        };
    }
}

sub PrepareUniqLists {
  my %uniq_dates = ();
  my @all_keys   = ();
  foreach my $base (keys %$STATS) {
    my $basestats = $STATS->{$base};
    foreach my $date (keys %$basestats) {
      next if $BEFORE_DATE && $date gt $BEFORE_DATE;
      next if $AFTER_DATE  && $date lt $AFTER_DATE;
      $uniq_dates{$date}++;
      push(@all_keys, [ $base, $date ]);
      $DIRECT_REPORT{"$base|$date"} = $basestats->{$date};
    }
  }

  @UNIQ_DATES = sort keys %uniq_dates;
  @UNIQ_BASES = sort keys %$STATS;
  @ALL_KEYS   = sort { $a->[0] cmp $b->[0] || $a->[1] cmp $b->[1] } @all_keys;
}

sub DumpReport {
  &PrepareUniqLists;

  my $by_totf = &AllKeysByValue('totf');
  my $by_incf = &AllKeysByValue('incf');
  my $by_tots = &AllKeysByValue('tots');
  my $by_incs = &AllKeysByValue('incs');

  foreach my $report (@REPORT_ORDER) {
    &ReportTop('= Top Usage By Total Files =======', $by_totf, 'totf') if $report eq 'totf';
    &ReportTop('= Top Usage By Incremental Files =', $by_incf, 'incf') if $report eq 'incf';
    &ReportTop('= Top Usage By Total Size ========', $by_tots, 'tots') if $report eq 'tots';
    &ReportTop('= Top Usage By Incremental Size ==', $by_incs, 'incs') if $report eq 'incs';
  }

}

sub ReportTop {
  my ($header,$list,$valname) = @_;

  print "\n",
        "=========$header=========\n";
  
  my $printed   = 0;
  my %seen_base = ();
  for (my $i=0; $printed < $NUM_ENTRIES && $i < @$list; $i++) {
    my $key          = $list->[$i];
    my ($base,$date) = @$key;
    if ($TOP_BASE_ONLY) {
       next if $seen_base{$base}++;
    }
    my $entry        = $DIRECT_REPORT{"$base|$date"};
    my $val          = $entry->{$valname};
    if ($valname =~ /s$/i) {
      if      ($val >= 1073741824) {
        $val = sprintf("%.2f Gibytes",$val / 1073741824);
      } elsif ($val >= 1048576) {
        $val = sprintf("%.2f Mibytes",$val / 1048576);
      } elsif ($val >= 1024) {
       $val = sprintf("%.2f Kibytes",$val / 1024);
      } else {
       $val = sprintf("%d bytes",$val);
      }
    }
    printf "%10s %-26s %14s\n",$date,$base,$val;
    $printed++;
  }
    
}

sub AllKeysByValue {
  my $valname = shift;
  my @sorted = sort {
                 my ($abase,$adate) = @$a;
                 my ($bbase,$bdate) = @$b;
                 my $ra = $DIRECT_REPORT{"$abase|$adate"};
                 my $rb = $DIRECT_REPORT{"$bbase|$bdate"};

                 $rb->{$valname} <=> $ra->{$valname}
                                 or
                          $abase cmp $bbase
                                 or
                          $bdate cmp $adate

               } @ALL_KEYS;
  \@sorted;
}

#sub DumpReportOld {
#  #$Data::Dumper::Indent=1;  # Fixed-size data dump indentation.
#  #$Data::Dumper::Terse=1;   # No $VARn prefix in dumps.
#  #my $stats = Data::Dumper->Dump( [ $STATS ] );
#  #print $stats,"\n";
#
#  &PrepareUniqLists;
#
#  #foreach my $base (@UNIQ_BASES) {
#  #  my $basestats = $STATS->{$base};
#  #  foreach my $date (@UNIQ_DATES) {
#  #    my $rep = $basestats->{$date}; # can: not exist
#  #    next unless $rep;
#
#  #    my @vals = ();
#  #    push(@vals, $rep->{'totf'}) if $DO_TOT_NUMFILES;
#  #    push(@vals, $rep->{'incf'}) if $DO_INC_NUMFILES;
#  #    push(@vals, $rep->{'tots'}) if $DO_TOT_SIZE;
#  #    push(@vals, $rep->{'incs'}) if $DO_INC_SIZE;
#
#  #    print $base,            $SEP,
#  #          $date,            $SEP,
#  #          join($SEP,@vals),
#  #          "\n";
#  #  }
#  #}
#
#
#  # Build headers
#
#  my @ext = ();
#  push(@ext, "_TotF") if $DO_TOT_NUMFILES;
#  push(@ext, "_IncF") if $DO_INC_NUMFILES;
#  push(@ext, "_TotS") if $DO_TOT_SIZE;
#  push(@ext, "_IncS") if $DO_INC_SIZE;
#  my @headers = ();
#  foreach my $base (@UNIQ_BASES) {
#    foreach my $ext (@ext) {
#      push(@headers,"$base$ext");
#    }
#  }
#  print "#Date", $SEP,
#        join($SEP,@headers),
#        "\n";
#
#  # Dump data by date
#
#  foreach my $date (@UNIQ_DATES) {
#    print $date, $SEP;
#
#    my @vals = ();
#
#    foreach my $base (@UNIQ_BASES) {
#      my $basestats = $STATS->{$base};
#      my $rep = $basestats->{$date} || {}; # can not exist
#
#      push(@vals, $rep->{'totf'} || "") if $DO_TOT_NUMFILES;
#      push(@vals, $rep->{'incf'} || "") if $DO_INC_NUMFILES;
#      push(@vals, $rep->{'tots'} || "") if $DO_TOT_SIZE;
#      push(@vals, $rep->{'incs'} || "") if $DO_INC_SIZE;
#
#    }
#
#    print join($SEP,@vals), "\n";
#  }
#
#  1;
#}

sub ValidateDate {
  my $date = shift;
  if ($date =~ /^-(\d+)$/) {
    my $ago = ParseDate("$1 days ago");
    my $tt  = UnixDate($ago,"%Y-%m-%d");
    print STDERR "Computed past date: $tt\n" if $DEBUG;
    return $tt;
  }
  if ($date !~ /^\d\d\d\d(-\d\d(-\d\d)?)?$/) {
    die "Illegal date format: should be YYYY or YYYY-MM or YYYY-MM-DD\n";
  }
  return $date;
}

