# Spotlake-Migration: AWS

This repository is code that move spot data from a timestream to other.

### How To Use
1. clone this repository
2. run 'setting.sh'
3. run migration codes you want
(optional) put latest file as 'latest.csv.gz' in this directory before running migration code

### Migration codes
* migration.py : Migration code with multi-processing
* migration_gap.py : Migration code for data gap between migration and collector
* migration_copy.py : Migration code to copy data from a Timestream DB to other in same account
* migration_loss.py : Migration code to upload data from S3 Bucket to Timestream DB
* migration_kmubigdata_to_spotrank : Migration code for data from Timestream DB in KMUBIGDATA account to Timestream DB in SPOTRANK account

