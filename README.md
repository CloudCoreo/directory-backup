# CloudCoreo Generic Directory Backup
You can use this to back up any directory to S3 on a cron basis. Included is the ability to roll backups in S3 as to not retain too many copies

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
