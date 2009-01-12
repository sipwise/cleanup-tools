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

# archive tables older than $ARCHIVE_MONTHS will be dumped to a file,
# gzipped and then dropped afterwards
ARCHIVE_MONTHS=12
# drop the dump files into $ARCHIVE_DIR
ARCHIVE_DIR=/tmp

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

# command shortcut
MYSQL="mysql -u${DBUSER} -p${DBPASS} -h${DBHOST} --skip-column-names"
MYSQLDUMP="mysqldump -u${DBUSER} -p${DBPASS} -h${DBHOST} --opt"

delete_loop() {
	DB=$1
	TABLE=$2
	MTABLE=$3
	COL=$4
	MSTART=$5

	while :; do
		RC=$(
			$MYSQL $DB -e "INSERT INTO $MTABLE SELECT * FROM $TABLE
				WHERE $COL >= '$MSTART'
				AND $COL < DATE_ADD('$MSTART', INTERVAL 1 MONTH) LIMIT 1000;
				SELECT ROW_COUNT()"
		)
		test $? = 0 || break

		test "$RC" = 0 && break

		$MYSQL $DB -e "DELETE $TABLE FROM $TABLE
			LEFT JOIN $MTABLE
			ON $TABLE.id = $MTABLE.id
			WHERE $MTABLE.id IS NOT NULL"
	done
}

archive_dump() {
	TABLES=$1
	DB=$2

	for TABLE in $TABLES; do
		MONTH=$ARCHIVE_MONTHS
		while :; do
			MTABLE="${TABLE}_$(date +%Y%m -d "$MONTH months ago")"
			STATUS=$($MYSQL $DB -e "SHOW TABLE STATUS LIKE '$MTABLE'")
			test -z "$STATUS" && break
			MONTH=$(($MONTH + 1))
			TARGET=$ARCHIVE_DIR/$MTABLE.$(date +%Y%m%d%H%M%S).sql
			if ! $MYSQLDUMP $DB $MTABLE > $TARGET; then
				echo "MySQL DUMP of table $MTABLE into file $TARGET failed" 1>&2
				continue
			fi
			if ! nice gzip -9 $TARGET; then
				echo "Gzipping of dump file $TARGET failed" 1>&2
				rm -f "$TARGET" "$TARGET".gz
				continue
			fi
			$MYSQL $DB -e "DROP TABLE $MTABLE"
		done
	done
}

backup_table() {
	TABLES=$1
	DB=$2
	COL=$3

	for CMONTH in `seq 0 $((MONTHS_BACK-1))`; do 
		TMONTHS=$((CMONTH+MONTHS))
		TSTAMPL=$(date +%Y-%m -d "${TMONTHS} months ago")
		TSTAMP=$(date +%Y%m -d "${TMONTHS} months ago")
		MSTART="$TSTAMPL-01 00:00:00"

		for TABLE in $TABLES; do
			MTABLE="${TABLE}_${TSTAMP}"

			$MYSQL $DB -e "CREATE TABLE IF NOT EXISTS $MTABLE LIKE $TABLE"

			delete_loop $DB $TABLE $MTABLE $COL $MSTART
		done
	done
}

########################################################################

PATH="/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin:/usr/local/bin"

backup_table "$ACC_TABLES" $ACC_DB time
backup_table "$CDR_TABLES" $CDR_DB start_time

archive_dump "$ACC_TABLES" $ACC_DB
archive_dump "$CDR_TABLES" $CDR_DB
