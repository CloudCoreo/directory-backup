#!/usr/bin/env python

######################################################################
## Cloudcoreo directory backup and restore
##   example:
##       python cloudcoreo-directory-backup.py \
##              --log-file /var/log/cloudcoreo-directory-backup.log \
##              --s3-backup-bucket <bucket name> \
##              --s3-backup-region <region> \
##              --s3-prefix <backup/prefix/in/s3/bucket> \
##              --directory <dir 1> \
##              --directory <dir 2> \
##              --exculdes "*.tmp" \
##              --pre-backup-script </path/to/script> \
##              --post-backup-script </path/to/script> \
##              --quiet-time 3600 \
##              --rolling-pattern <hours, days, weeks, months, years> \
##              --restore
##              --dump-dir </path/to/script>
##
######################################################################
import os, sys, stat
import math
from filechunkio import FileChunkIO
import string
import tarfile
import boto
import datetime
import subprocess
from subprocess import call
import ConfigParser
import logging
from optparse import OptionParser
import argparse
import textwrap
from contextlib import closing

version = '0.0.3'

logging.basicConfig()
def parseArgs():
    parser = argparse.ArgumentParser(
        prog='manage-icbackend.py',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
 Cloudcoreo directory backup and restore
   example:
       python cloudcoreo-directory-backup.py \
              --log-file /var/log/cloudcoreo-directory-backup.log \
              --s3-backup-bucket <bucket name> \
              --s3-backup-region <region> \
              --s3-prefix <backup/prefix/in/s3/bucket> \
              --directory <dir 1> \
              --directory <dir 2> \
              --pre-backup-script </path/to/script> \
              --post-backup-script </path/to/script> \
              --quiet-time 3600 \
              --rolling-pattern <hours, days, weeks, months> \
              --restore \ ## this will override all other options that are unncessary
              --dump-dir </path/to/script>
'''
    ))
    parser.add_argument("--log-file",                dest="logFile",                                     default="/var/log/cloudcoreo-directory-backup.log", required=False, help="The log file in which to dump debug information [default: %default]")
    parser.add_argument("--s3-backup-bucket",        dest="s3BackupBucket",                              default=None,                                       required=False,  help="The s3 bucket where the directories should be backed up [default: %default]")
    parser.add_argument("--s3-backup-region",        dest="s3BackupRegion",                              default=None,                                       required=False,  help="The regoin where the s3 backup bucket exists [default: %default]")
    parser.add_argument("--s3-prefix",               dest="s3Prefix",                                    default=None,                                       required=False,  help="The key prefix in s3... this will be <key>/<timestamp>/ [default: %default]")
    parser.add_argument("--directory",               dest="backupDirectories",     action="append",      default=[],                                         required=False,  help="Specified one or more times to determine which directories must be backed up")
    parser.add_argument("--pre-backup-script",       dest="preBackupScript",                             default=None,                                       required=False, help="A script to run blindly (./<script>) before tar-gzipping the backup directories")
    parser.add_argument("--post-backup-script",      dest="postBackupScript",                            default=None,                                       required=False, help="A script to run blindly (./<script>) after tar-gzipping the backup directories, but before syncing to s3")
    parser.add_argument("--rolling-pattern",         dest="rollingPattern",                              default="24,7,5,12",                                required=False, help="A CSV of how many backups of each type to keep. I.E 24,7,5,12 will keep 24 hourly, 7 daily, 5 weekly, and 12 monthly")
    parser.add_argument("--restore",                 dest="restore",               action="store_true",  default=False,                                      required=False, help="Perform a restore")
    parser.add_argument("--dump-dir",                dest="dumpDir",                                     default="/tmp/backup-dump",                         required=False, help="Where to store the tar.gz files before uploading to s3")
    parser.add_argument("--pre-restore-script",      dest="preRestoreScript",                            default=None,                                       required=False, help="A script to run blindly (./<script>) before restoring the latest backup")
    parser.add_argument("--post-restore-script",     dest="postRestoreScript",                           default=None,                                       required=False, help="A script to run blindly (./<script>) after restoring the latest backup")
    parser.add_argument("--debug",                   dest="debug",                 action="store_true",  default=False,                                      required=False, help="Whether or not to run the app in debug mode [default: %default]")
    parser.add_argument("--version",                 dest="version",               action="store_true",  default=False,                                      required=False, help="Display the current version")
    return parser.parse_args()

def log(statement):
    statement = str(statement)
    if options.logFile is None:
        return
    if not os.path.exists(os.path.dirname(options.logFile)):
        os.makedirs(os.path.dirname(options.logFile))
    logFile = open(options.logFile, 'a')
    ts = datetime.datetime.now()
    isFirst = True
    for line in statement.split("\n"):
        if isFirst:
            if options.debug:
                print("%s - %s\n" % (ts, line))
            else:
                logFile.write("%s - %s\n" % (ts, line))
            isFirst = False
        else:
            if options.debug:
                print("%s -    %s\n" % (ts, line))
            else:
                logFile.write("%s -    %s\n" % (ts, line))
    logFile.close()

def getAvailabilityZone():
    ## cached
    global MY_AZ
    if MY_AZ is None:
        if options.debug:
            MY_AZ = 'us-east-1a'
        else:
            MY_AZ = metaData("placement/availability-zone")
    return MY_AZ
    
def getRegion():
    region = getAvailabilityZone()[:-1]
    log("region: %s" % region)
    return region

def metaData(dataPath):
    ## using 169.254.169.254 instead of 'instance-data' because some people
    ## like to modify their dhcp tables...
    return requests.get('http://169.254.169.254/latest/meta-data/%s' % dataPath).text

def runScript(script, onFailure = ""):
    if os.path.isfile(script) != True:
        error("Script [%s] was not found" % script)
    log("running script [%s]" % script)
    os.chmod(script, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
    ## we need to check the error and output if we are debugging or not
    err = None
    out = None
    proc = subprocess.Popen([script], stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    log(err)
    log(out)
    if not err:
        ## return the return code
        log("Success running script [%s]" % script)
        log("  returning rc [%d]" % proc.returncode)
    else:
        fullRunError = err
        exec onFailureString
    return proc.returncode
    
def error(message):
    log("ERROR: %s" % message)
    raise Exception(message)

def restoreDirectories():
    backupfiles = getBackupFiles()
    if options.dumpDir == None or os.path.exists(options.dumpDir) == False:
        error("invalid dump dir [%s]" % options.dumpDir)
    bucket = getS3BackupBucket()
    for directory in options.backupDirectories:
        log("working on directory: %s" % directory)
        filename = "%s/%s.tar.gz" % (options.dumpDir, directory.replace(os.path.sep, "_"))

    tar = tarfile.open("sample.tar.gz")
    tar.extractall()
    tar.close()

def runBackup():
    backupfiles = []
    if options.dumpDir == None or os.path.exists(options.dumpDir) == False:
        error("invalid dump dir [%s]" % options.dumpDir)
    for directory in options.backupDirectories:
        log("working on directory: %s" % directory)
        filename = "%s/%s.tar.gz" % (options.dumpDir, directory.replace(os.path.sep, "_"))
        log("creating file: %s" % filename)
        if os.path.exists(directory) == False:
            error("invalid directory specified")
        with closing(tarfile.open(filename, "w:gz")) as tar:
            tar.add(directory, arcname=os.path.basename(directory))
        backupfiles.append(filename)
    log("created archive [%s]" % filename)
    return backupfiles

def runPreRestoreStripts():
    if options.preRestoreScript:
        return runScript(options.preRestoreScript, onFailure = "sys.exit(1)")
    else:
        return 0

def runPostRestoreStripts():
    if options.preRestoreScript:
        return runScript(options.postRestoreScript, onFailure = "sys.exit(1)")
    else:
        return 0

def getS3BackupBucket():
    # Returns the boto S3 Bucket object being used for backups
    log("connecting to region: %s" % options.s3BackupRegion)
    log("connecting to bucket: %s" % options.s3BackupBucket)
    return s3.get_bucket(options.s3BackupBucket)

def getBackupFiles():
    ## Get a sorted list of backup files and arranged by how old they are (hourly, daily, weekly, monthly)
    backup_files = sorted(getS3BackupBucket().list(options.s3Prefix+"/"), reverse=True, key=lambda s3_key: s3_key.name)
    backup_list={"hourly": [], "daily": [], "weekly": [], "monthly": []}
    now=datetime.datetime.now()
    day_delta=timedelta(days=1)
    week_delta=timedelta(days=7)
    month_delta=timedelta(days=31) # close enough to a month
    for backup_key in backup_files:
        if(re.match(".*\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}/", backup_key.name)):
            date_ext=backup_key.name.split("/")[-2]
            key_dir = dirname(backup_key.name)
            (year, month, day, hour, mins, sec) = date_ext.split("-")
            backup_date=datetime.datetime(int(year), int(month), int(day), int(hour), int(mins), int(sec))
            if backup_date > now - day_delta:
                if key_dir not in backup_list["hourly"]:
                    backup_list["hourly"].append(key_dir)
            elif backup_date > now - week_delta:
                if key_dir not in backup_list["daily"]:
                    backup_list["daily"].append(key_dir)
            elif backup_date > now - month_delta:
                if key_dir not in backup_list["weekly"]:
                    backup_list["weekly"].append(key_dir)
            else:
                if key_dir not in backup_list["monthly"]:
                    backup_list["monthly"].append(key_dir)
        else:
            log("Found something we didn't expect: %s " % backup_key.name)
    return backup_list

def cleanupOldBackups(hourly=25, daily=8, weekly=6, monthly=6):
    bucket = getS3BackupBucket()
    ## Keeps one days worth of hourly backups, one week of daily backups, one month of weekly backups, and one year of monthly backups
    log("Cleaning up older backup files that are no longer needed.")
    backup_policy={"hourly": hourly, "daily": daily, "weekly": weekly, "monthly": monthly}
    backup_files = getBackupFiles()
    for time_period in backup_files:
        backups = backup_files[time_period]
        while len(backups) > backup_policy[time_period]:
            backup_name = backups.pop()
            log("Removing old backup %s from s3 bucket." % backup_name)
            try:
                bucketListResultSet = bucket.list(prefix=backup_name)
                result = bucket.delete_keys([key.name for key in bucketListResultSet])
            except:
                log("Couldn't delete backup from s3 %.  Exception: %." % (backup_name, traceback.format_exc()))

def uploadToS3(localFile, s3_key):
    bucket = getS3BackupBucket()
    # Upload file to s3 bucket
    log("uploading [%s] to s3 bucket: %s" % (localFile, bucket))
    source_size = os.stat(localFile).st_size
    log("source_size: %d" % source_size)
    mp = bucket.initiate_multipart_upload(s3_key, encrypt_key=True)
    chunk_size = 52428800
    chunk_count = int(math.ceil(source_size / chunk_size))
    # Send the file parts, using FileChunkIO to create a file-like object
    # that points to a certain byte range within the original file. We
    # set bytes to never exceed the original file size
    for i in range(chunk_count + 1):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(localFile, 'r', offset=offset, bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)
    # Finish the upload
    mp.complete_upload()
    
def main():
    ## lets make sure the directories are valid
    for directory in options.backupDirectories:
        if os.path.exists(directory) == False:
            error("invalid directory specified [%s]" % directory)

    ## basic premise is this:
    ##   run a restore check on first launch... 
    ##     failure exits the script
    ##     script success means restore
    ##       run the pre-restore if it exists
    ##       restore
    ##       run the post-restore if it exists
    ##   run the pre-backup if it exists
    ##     do not continue on error
    ##   run the backup (tar gz)
    ##   run the post backup if it exists
    ##     error is logged but script contineues
    ##   upload to s3

    ##   run a restore check on first launch... 
    if options.restore == True
        ## run the pre-restore if it exists
        if runPreRestoreStripts() == 0:
            ## restore if prerestore is ok
            restoreDirectories()
            ## run the post-restore if it exists
            runPostRestoreStripts()
        else:
            error("pre restore script exited with code [%d].. exiting" % rc)
        sys.exit(0)
    
    ##   run the pre-backup if it exists
    if options.preBackupScript:
        ## do not continue on error
        rc = runScript(options.preBackupScript, onFailure = "sys.exit(1)")
        if rc != 0:
            sys.exit(1)
    ## run the backup (tar gz)
    tar_files = runBackup()

    ##   run the post backup if it exists
    if options.postBackupScript:
        ## continue on error
        rc = runScript(options.postBackupScript, onFailure = "")
        if rc != 0:
            ## error is logged but script contineues
            log("Post backup did not execute succesfully")
        else:
            log("Post backup executed succesfully")

    ## upload to s3
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    log("timestamp: %s" % timestamp)
    for tar_file in tar_files:
        s3_backup_key = "%s/%s/%s" % (options.s3Prefix, timestamp, os.path.basename(tar_file))
        log("s3_backup_key: %s" % s3_backup_key)
        uploadToS3(tar_file, s3_backup_key)

options = parseArgs()

if options.version:
    print version
    sys.exit(0)

log("connecting to s3 region %s" % options.s3BackupRegion)
s3 = boto.s3.connect_to_region(options.s3BackupRegion)

main()
