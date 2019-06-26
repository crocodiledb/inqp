#!/bin/bash
PG_HOME=/home/totemtang/IQP/postgresql
BIN_HOME=$PG_HOME/pgsql_install/bin

TPCH_SCRIPTS=/home/totemtang/slothdb/pg_verification
LINEITEM=/home/totemtang/slothdb/spark/middle-ground/tpchdata/lineitem/lineitem.tbl
ORDERS=/home/totemtang/slothdb/spark/middle-ground/tpchdata/orders/orders.tbl

# echo "Creating tables"

opts="-d slothdb"

# $BIN_HOME/psql $opts -f $TPCH_SCRIPTS/tpch_create.sql

#$BIN_HOME/psql $opts -c "COPY lineitem FROM '$LINEITEM' WITH (FORMAT csv, DELIMITER '|')"
$BIN_HOME/psql $opts -c "COPY orders FROM '$ORDERS' WITH (FORMAT csv, DELIMITER '|')"

