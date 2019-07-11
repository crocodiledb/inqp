#!/bin/bash
PG_HOME=/home/totemtang/IQP/postgresql
BIN_HOME=$PG_HOME/pgsql_install/bin

TPCH_SCRIPTS=/home/totemtang/slothdb/pg_verification
DATA_DIR=/home/totemtang/slothdb/slothdb_testsuite/datadir/tpchdata
LINEITEM=$DATA_DIR/lineitem/lineitem.tbl
ORDERS=$DATA_DIR/orders/orders.tbl
CUSTOMER=$DATA_DIR/customer/customer.tbl
PART=$DATA_DIR/part/part.tbl
SUPPLIER=$DATA_DIR/supplier/supplier.tbl
PARTSUPP=$DATA_DIR/partsupp/partsupp.tbl
NATION=$DATA_DIR/nation/nation.tbl
REGION=$DATA_DIR/region/region.tbl

# echo "Creating tables"

opts="-d slothdb"

# $BIN_HOME/psql $opts -f $TPCH_SCRIPTS/tpch_create.sql

#$BIN_HOME/psql $opts -c "COPY lineitem FROM '$LINEITEM' WITH (FORMAT csv, DELIMITER '|')"
#$BIN_HOME/psql $opts -c "COPY orders FROM '$ORDERS' WITH (FORMAT csv, DELIMITER '|')"
#$BIN_HOME/psql $opts -c "COPY part FROM '$PART' WITH (FORMAT csv, DELIMITER '|')"

#$BIN_HOME/psql $opts -c "COPY customer FROM '$CUSTOMER' WITH (FORMAT csv, DELIMITER '|')"
$BIN_HOME/psql $opts -c "COPY supplier FROM '$SUPPLIER' WITH (FORMAT csv, DELIMITER '|')"
#$BIN_HOME/psql $opts -c "COPY partsupp FROM '$PARTSUPP' WITH (FORMAT csv, DELIMITER '|')"
#$BIN_HOME/psql $opts -c "COPY nation FROM '$NATION' WITH (FORMAT csv, DELIMITER '|')"
#$BIN_HOME/psql $opts -c "COPY region FROM '$REGION' WITH (FORMAT csv, DELIMITER '|')"


