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

# command shortcut
MYSQL="mysql -u${DBUSER} -p${DBPASS} -h${DBHOST} --skip-column-names"

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

		test "$RC" = 0 && break

		$MYSQL $DB -e "DELETE $TABLE FROM $TABLE
			LEFT JOIN $MTABLE
			ON $TABLE.id = $MTABLE.id
			WHERE $MTABLE.id IS NOT NULL"
	done
}

########################################################################

PATH="/sbin:/usr/sbin:/usr/local/sbin:/bin:/usr/bin:/usr/local/bin"
for CMONTH in `seq 0 $((MONTHS_BACK-1))`; do 

	TMONTHS=$((CMONTH+MONTHS))
	TSTAMPL=$(date +%Y-%m -d "${TMONTHS} months ago")
	TSTAMP=$(date +%Y%m -d "${TMONTHS} months ago")
	MSTART="$TSTAMPL-01 00:00:00"

	for TABLE in ${ACC_TABLES}; do
		MTABLE="${TABLE}_${TSTAMP}"

		$MYSQL $ACC_DB -e "CREATE TABLE IF NOT EXISTS $MTABLE LIKE $TABLE"

		delete_loop $ACC_DB $TABLE $MTABLE time $MSTART
	done

	for TABLE in ${CDR_TABLES}; do
		MTABLE="${TABLE}_${TSTAMP}"

		$MYSQL $CDR_DB -e "CREATE TABLE IF NOT EXISTS $MTABLE LIKE $TABLE"

		delete_loop $CDR_DB $TABLE $MTABLE start_time $MSTART
	done
done
