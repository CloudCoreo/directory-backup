# CloudCoreo Generic Directory Backup
You can use this to back up any directory to S3 on a cron basis. Included is the ability to roll backups in S3 as to not retain too many copies

## Installation
There is a `requirements.txt` file in the root that can be pip installed:
`pip install -r requirements.txt`

If you prefer, the it is included as well in CloudCoreo's RPM repository. The RPM repo can be installed via:
```
rpm -ivh https://s3.amazonaws.com/cloudcoreo-yum/repo/tools/cloudcoreo-0.0.1-1.noarch.rpm 
yum makecache
```
Then simply run:
```
yum install -y cloudcoreo-directory-backup
```
and the file will be install in /opt/

```
Cloudcoreo directory backup and restore
  example:
      python cloudcoreo-directory-backup.py \
             --log-file /var/log/cloudcoreo-directory-backup.log \
             --s3-backup-bucket <bucket name> \
             --s3-backup-region <region> \
             --s3-prefix <backup/prefix/in/s3/bucket> \
             --directory <dir 1> \
             --directory <dir 2> \
             --exculde ".*\.tmp" \
             --pre-backup-script </path/to/script> \
             --post-backup-script </path/to/script> \
             --quiet-time 3600 \
             --rolling-pattern <hours, days, weeks, months, years> \
             --restore
             --dump-dir </path/to/script>
```