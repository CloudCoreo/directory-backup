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
##              --exculde ".*\.tmp" \
##              --pre-backup-script </path/to/script> \
##              --post-backup-script </path/to/script> \
##              --quiet-time 3600 \
##              --rolling-pattern <hours, days, weeks, months, years> \
##              --restore
##              --dump-dir </path/to/script>
##
######################################################################
from posixpath import dirname
import os, sys, stat
import math
from filechunkio import FileChunkIO
import string
import tarfile
import boto
from boto.s3.connection import OrdinaryCallingFormat
import datetime
import subprocess
from subprocess import call
import ConfigParser
import logging
from optparse import OptionParser
import argparse
import textwrap
from contextlib import closing
from datetime import timedelta
import re

version = '0.0.11'

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
              --exclude <regex pattern 1> \
              --exclude <regex pattern 2> \
              --pre-backup-script </path/to/script> \
              --post-backup-script </path/to/script> \
              --quiet-time 3600 \
              --rolling-pattern <hours, days, weeks, months, years> \
              --restore \ ## this will override all other options that are unncessary
              --dump-dir </path/to/script>
'''
    ))
    parser.add_argument("--log-file",                dest="logFile",                                     default="/var/log/cloudcoreo-directory-backup.log", required=False, help="The log file in which to dump debug information [default: %default]")
    parser.add_argument("--s3-backup-bucket",        dest="s3BackupBucket",                              default=None,                                       required=False, help="The s3 bucket where the directories should be backed up [default: %default]")
    parser.add_argument("--s3-backup-region",        dest="s3BackupRegion",                              default=None,                                       required=False, help="The regoin where the s3 backup bucket exists [default: %default]")
    parser.add_argument("--s3-prefix",               dest="s3Prefix",                                    default=None,                                       required=False, help="The key prefix in s3... this will be <key>/<timestamp>/ [default: %default]")
    parser.add_argument("--directory",               dest="backupDirectories",     action="append",      default=[],                                         required=False, help="Specified one or more times to determine which directories must be backed up")
    parser.add_argument("--exclude",                 dest="excludes",              action="append",      default=[],                                         required=False, help="Patterns to exclude")
    parser.add_argument("--pre-backup-script",       dest="preBackupScript",                             default=None,                                       required=False, help="A script to run blindly (./<script>) before tar-gzipping the backup directories")
    parser.add_argument("--post-backup-script",      dest="postBackupScript",                            default=None,                                       required=False, help="A script to run blindly (./<script>) after tar-gzipping the backup directories, but before syncing to s3")
    parser.add_argument("--rolling-pattern",         dest="rollingPattern",                              default="24,7,5,12,5",                              required=False, help="A CSV of how many backups of each type to keep. I.E 24,7,5,12,5 will keep 24 hourly, 7 daily, 5 weekly, 12 monthly and 5 yearly")
    parser.add_argument("--restore",                 dest="restore",               action="store_true",  default=False,                                      required=False, help="Perform a restore")
    parser.add_argument("--restore-stamp",           dest="restoreStamp",                                default=None,                                       required=False, help="The timestamp to restore - defaults to the lastest hourly backup")
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
                print("%s - %s" % (ts, line))
            else:
                logFile.write("%s - %s\n" % (ts, line))
            isFirst = False
        else:
            if options.debug:
                print("%s -    %s" % (ts, line))
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
    proc_ret_code = None
    run = []
    run.append(script)
    with open(options.logFile, 'a') as log_file:
        proc_ret_code = subprocess.call(run, shell=False, stdout=log_file, stderr=log_file)

    if proc_ret_code == 0:
        ## return the return code
        log("Success running script [%s]" % script)
        log("  returning rc [%d]" % proc_ret_code)
    else:
        exec onFailure
    return proc_ret_code
    
def error(message):
    log("ERROR: %s" % message)
    raise Exception(message)

def restoreDirectories():
    backupFiles = getBackupFiles()
    log("backup files: %s" % backupFiles)
    backupKey = None
    ## try to restore the timestamp they asked for
    if options.restoreStamp:
        for key, stampType in backupFiles.iteritems():
            for backupDir in stampType:
                log("  backupDir: %s" % backupDir)
                if(backupDir.split('/')[:-1] == options.restoreStamp):
                    backupKey = backupDir
        if backupKey == None:
            log("restoreStamp was not found - restoring the latest")
    
    ## if we don't have a backup key, got get the lastest backup
    if backupKey == None:
        if backupFiles['hourly'] and backupFiles['hourly'][0]:
            backupKey = backupFiles['hourly'][0]
        elif backupFiles['daily'] and backupFiles['daily'][0]:
            backupKey = backupFiles['daily'][0]
        elif backupFiles['weekly'] and backupFiles['weekly'][0]:
            backupKey = backupFiles['weekly'][0]
        elif backupFiles['monthly'] and backupFiles['monthly'][0]:
            backupKey = backupFiles['monthly'][0]

    ## if our key is still none at this point, we have never performed a backup - just return
    if backupKey == None:
        return

    if options.dumpDir == None or os.path.exists(options.dumpDir) == False:
        error("invalid dump dir [%s]" % options.dumpDir)
    bucket = getS3BackupBucket()
    log("restore got bucket: %s" % bucket)
    for directory in options.backupDirectories:
        log("working on directory: %s" % directory)
        filename = "%s/%s.tar.gz" % (options.dumpDir, directory.replace(os.path.sep, "_"))
        s3Key = "%s/%s" % (backupKey, filename.split('/')[-1])
        log("  s3Key: %s" % s3Key)
        log("  filename: %s" % filename)
        log("  downloadFromS3(%s, %s)" % (s3Key, filename))
        downloadFromS3(s3Key, filename)
        tar = tarfile.open(filename)
        tar.extractall(path = os.path.abspath(os.path.join(directory, os.pardir)))
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
            tar.add(directory, arcname=os.path.basename(directory), exclude=exclude_function)
        backupfiles.append(filename)
    log("created archive [%s]" % filename)
    return backupfiles

def exclude_function(tarinfo):
    for regex in options.excludes:
        matcher = re.compile(regex)
        if matcher.match(tarinfo):
            log("skipping file: %s" % tarinfo)
            return True
    log("adding file: %s" % tarinfo)
    return False

def getS3BackupBucket():
    # Returns the boto S3 Bucket object being used for backups
    log("connecting to region: %s" % options.s3BackupRegion)
    log("connecting to bucket: %s" % options.s3BackupBucket)
    return s3.get_bucket(options.s3BackupBucket)

def getAllBackupBucketMatchingFiles():
    backup_files = sorted(getS3BackupBucket().list(options.s3Prefix+"/"), reverse=True, key=lambda s3_key: s3_key.name)
    allFiles = []
    x = 0;
    for backup_key in backup_files:
        if(re.match(".*\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}/", backup_key.name)):
            allFiles.append(backup_key)
        else:
            log("Found something we didn't expect: %s " % backup_key.name)

    log("found %i backup files in the bucket" % len(allFiles))
    return allFiles

def getBackupFiles():
    ## this is the heart of the cleanup process.
    ## logic:
    ## get the latest hourlys
    ##   get the latest daily's that are not part of the hourlys
    ##     get the latest weeklys that are not part of the hourlys or dailys
    ##       get the latest monthlys that are not part of the hourlys or dailys or weeklys
    ##         get the latest yearlys that are not part of the hourlys or dailys or weeklys or monthlys
    ## Get a sorted list of backup files and arranged by how old they are (hourly, daily, weekly, monthly)
    backup_files = getAllBackupBucketMatchingFiles()
    backup_list={"hourly": [], "daily": [], "weekly": [], "monthly": [], "yearly": [], "other": []}
    backup_date_list={"hourly": [], "daily": [], "weekly": [], "monthly": [], "yearly": []}
    for backup_key in backup_files:
        date_ext=backup_key.name.split("/")[-2]
        key_dir = dirname(backup_key.name)
        (year, month, day, hour, mins, sec) = date_ext.split("-")
        for time_interval in backup_list:
            if time_interval == "hourly":
                backup_date=datetime.datetime(int(year), int(month), int(day), int(hour), int(1), int(1))
                if backup_date not in backup_date_list[time_interval]:
                    backup_list[time_interval].append(key_dir)
                    backup_date_list[time_interval].append(backup_date)
            if time_interval == "daily":
                backup_date=datetime.datetime(int(year), int(month), int(day), int(1), int(1), int(1))
                if backup_date not in backup_date_list[time_interval]:
                    backup_list[time_interval].append(key_dir)
                    backup_date_list[time_interval].append(backup_date)
            if time_interval == "weekly":
                if int(day) % 7 == 0:
                    backup_date=datetime.datetime(int(year), int(month), int(day), int(1), int(1), int(1))
                    if backup_date not in backup_date_list[time_interval]:
                        backup_list[time_interval].append(key_dir)
                        backup_date_list[time_interval].append(backup_date)
            if time_interval == "monthly":
                backup_date=datetime.datetime(int(year), int(month), int(1), int(1), int(1), int(1))
                if backup_date not in backup_date_list[time_interval]:
                    backup_list[time_interval].append(key_dir)
                    backup_date_list[time_interval].append(backup_date)
            if time_interval == "yearly":
                backup_date=datetime.datetime(int(year), int(1), int(1), int(1), int(1), int(1))
                if backup_date not in backup_date_list[time_interval]:
                    backup_list[time_interval].append(key_dir)
                    backup_date_list[time_interval].append(backup_date)
    return backup_list

def cleanupOldBackups(hourly=25, daily=8, weekly=6, monthly=6, yearly=5):
    bucket = getS3BackupBucket()
    ## Keeps one days worth of hourly backups, one week of daily backups, one month of weekly backups, and one year of monthly backups
    ## logic:
    ##  get now through $hourly hours ago
    ##  get (now through $hourly hours) through (now - daily days)
    ##    iterate through all and keep newest of each day stamp
    ##  get (now through $daily days) through (now - monthly days)
    ##    iterate through all and keep newest of each month up to $montly
    ##  get (now thorugh $monthly months) through (now - yearly years)
    ##    iterate through all and keep the newset of each year up to $yearly

    log("Cleaning up older backup files that are no longer needed.")
    allBackupFiles = getAllBackupBucketMatchingFiles() ## this is a list of all files
    ## now we need to limit the ones to keep based on policy,
    ## put them all into a list and delete the ones that don't match.
    backupFiles = filterBackupFiles(getBackupFiles())
    
    for f in allBackupFiles:
        bFile = dirname(f.name)
        if bFile not in backupFiles:
            try:
                log("deleting old backup: %s" % bFile)
                bucketListResultSet = bucket.list(prefix=bFile)
                result = bucket.delete_keys([key.name for key in bucketListResultSet])
            except:
                log("Couldn't delete backup from s3 %.  Exception: %." % (bFile, traceback.format_exc()))
        else:
            log('not deleting relevant bFile in backup_files: %s' % bFile)

def filterBackupFiles(all_backup_files):
    filterdBackupFiles = []
    (num_hours, num_days, num_weeks, num_months, num_years) = options.rollingPattern.split(",")
    filterdBackupFiles.extend(all_backup_files["yearly"][0:int(num_years)])
    log('adding yearlys: %s' % len(all_backup_files["yearly"][0:int(num_years)]))
    for a in all_backup_files["yearly"][0:int(num_years)]:
        log('saving years: %s' % a)
    filterdBackupFiles.extend(all_backup_files["monthly"][0:int(num_months)])
    log('adding monthly: %s' % len(all_backup_files["monthly"][0:int(num_months)]))
    for a in all_backup_files["monthly"][0:int(num_months)]:
        log('saving months: %s' % a)
    filterdBackupFiles.extend(all_backup_files["weekly"][0:int(num_weeks)])
    log('adding weekly: %s' % len(all_backup_files["weekly"][0:int(num_weeks)]))
    for a in all_backup_files["weekly"][0:int(num_weeks)]:
        log('saving weekly: %s' % a)
    filterdBackupFiles.extend(all_backup_files["daily"][0:int(num_days)])
    log('adding daily: %s' % len(all_backup_files["daily"][0:int(num_days)]))
    for a in all_backup_files["daily"][0:int(num_days)]:
        log('saving daily: %s' % a)
    filterdBackupFiles.extend(all_backup_files["hourly"][0:int(num_hours)])
    log('adding hourly: %s' % len(all_backup_files["hourly"][0:int(num_hours)]))
    for a in all_backup_files["hourly"][0:int(num_hours)]:
        log('saving hourly: %s' % a)
    return filterdBackupFiles

def downloadFromS3(s3_key, localFile):
    bucket = getS3BackupBucket()
    log("downloading [%s] from s3 bucket: %s" % (localFile, bucket))
    key = bucket.get_key(s3_key)
    key.get_contents_to_filename(localFile)

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
    if options.restore == True:
        ## run the pre-restore if it exists
        preRestoreRc = runScript(options.preRestoreScript, onFailure = "sys.exit(1)")
        if preRestoreRc == 0:
            ## restore if prerestore is ok
            restoreDirectories()
            ## run the post-restore if it exists
            postRestoreRc = runScript(options.postRestoreScript, onFailure = "sys.exit(1)")
            if preRestoreRc != 0:
                sys.exit(preRestoreRc)
        else:
            error("pre restore script exited with code [%d].. exiting" % rc)
    else:
        ## lets make sure the directories are valid
        for directory in options.backupDirectories:
            if os.path.exists(directory) == False:
                error("invalid directory specified [%s]" % directory)
        
        ##   run the pre-backup if it exists
        if options.preBackupScript:
            ## do not continue on error
            rc = runScript(options.preBackupScript, onFailure = "sys.exit(1)")
            if rc != 0:
                sys.exit(rc)
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
        cleanupOldBackups()

options = parseArgs()

if options.version:
    print version
    sys.exit(0)

log("connecting to s3 region %s" % options.s3BackupRegion)
s3 = boto.s3.connect_to_region(options.s3BackupRegion, calling_format=OrdinaryCallingFormat())

main()
