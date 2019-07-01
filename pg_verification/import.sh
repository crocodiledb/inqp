#!/bin/bash
PG_HOME=/home/totemtang/IQP/postgresql
BIN_HOME=$PG_HOME/pgsql_install/bin

TPCH_SCRIPTS=/home/totemtang/slothdb/pg_verification
DATA_DIR=/home/totemtang/slothdb/slothdb_testsuite/datadir/tpchdata
LINEITEM=$DATA_DIR/lineitem/lineitem.tbl
ORDERS=$DATA_DIR/orders/orders.tbl
PART=$DATA_DIR/part/part.tbl

# echo "Creating tables"

opts="-d slothdb"

# $BIN_HOME/psql $opts -f $TPCH_SCRIPTS/tpch_create.sql

#$BIN_HOME/psql $opts -c "COPY lineitem FROM '$LINEITEM' WITH (FORMAT csv, DELIMITER '|')"
#$BIN_HOME/psql $opts -c "COPY orders FROM '$ORDERS' WITH (FORMAT csv, DELIMITER '|')"

$BIN_HOME/psql $opts -c "COPY part FROM '$PART' WITH (FORMAT csv, DELIMITER '|')"


