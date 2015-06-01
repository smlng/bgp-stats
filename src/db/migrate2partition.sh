YEARS="2005 2006 2007 2008"
MONTHS="01 02 03 04 05 06 07 08 09 10 11 12"
for i in $YEARS; do
  for j in $MONTHS ; do
    echo " migrate ${i}-${j}"
    echo " . create partition table"
    psql -c "CREATE TABLE IF NOT EXISTS t_origins_${i}_${j} () INHERITS (t_origins);" bgp.origins.rv_eqix
    echo " . export data to temp file"
    psql -c "COPY (SELECT o.dataset_id, o.prefix_id, o.asn FROM \
    (SELECT id FROM t_datasets WHERE date_trunc('month', ts) = '${i}-${j}-01') AS d \
    JOIN t_origins_full AS o ON d.id = o.dataset_id) \
    TO '/tmp/t_origins_part.copy' DELIMITER ';'; " bgp.origins.rv_eqix
    echo " . import data from temp file"
    psql -c "COPY t_origins_${i}_${j} FROM '/tmp/t_origins_part.copy' DELIMITER ';';" bgp.origins.rv_eqix
  done
done
