#!/bin/sh

# backing up all data older than $MONTHS months
# Note: if set for example to 1, this will backup the overall month,
# not only the records older than 30 days. This logic applies generally,
# so it will go back $MONTHS months, and starting from there, backing up
# the full months back to $MONTHS_BACK
MONTHS=3
# from the starting month above, also go back $MONTHS_BACK months and
# backup them
MONTHS_BACK=6

# accounting database on proxies (openser) or db1 (accounting)
ACC_DB="accounting"
# accounting tables on proxies (acc) or db1 (acc acc_backup acc_trash)
ACC_TABLES="acc acc_backup acc_trash"
# cdr database (accounting)
CDR_DB="accounting"
# cdr tables on proxies (empty) or db1 (cdr)
CDR_TABLES="cdr"

# DB access credentials
DBUSER="root"
DBPASS="1freibier!"
DBHOST="localhost"

########################################################################

PATH="/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin:/usr/local/bin"A
for CMONTH in `seq 0 $((MONTHS_BACK-1))`; do 

TMONTHS=$((CMONTH+MONTHS))
TSTAMP=$(date +%Y%m -d "${TMONTHS} months ago")

for TABLE in ${ACC_TABLES}; do
	mysql -u${DBUSER} -p${DBPASS} -h${DBHOST} ${ACC_DB} <<EOF
		CREATE TABLE IF NOT EXISTS ${TABLE}_${TSTAMP} LIKE ${TABLE};
		START TRANSACTION;
		INSERT INTO ${TABLE}_${TSTAMP} SELECT * FROM ${TABLE}
			WHERE EXTRACT(YEAR_MONTH FROM time)='${TSTAMP}';
		DELETE FROM ${TABLE} 
			WHERE EXTRACT(YEAR_MONTH FROM time)='${TSTAMP}';
		COMMIT;
EOF
done

for TABLE in ${CDR_TABLES}; do
	mysql -u${DBUSER} -p${DBPASS} -h${DBHOST} ${CDR_DB} <<EOF
		CREATE TABLE IF NOT EXISTS ${TABLE}_${TSTAMP} LIKE ${TABLE};
		START TRANSACTION;
		INSERT INTO ${TABLE}_${TSTAMP} SELECT * FROM ${TABLE}
			WHERE EXTRACT(YEAR_MONTH FROM start_time)='${TSTAMP}';
		DELETE FROM ${TABLE} 
			WHERE EXTRACT(YEAR_MONTH FROM start_time)='${TSTAMP}';
		COMMIT;
EOF
done

done
