YEARS="2005 2006 2007"
MONTHS="02 03 04"
for i in $YEARS; do
  for j in $MONTHS ; do
    psql -c "SELECT o.dataset_id, o.prefix_id, o.asn FROM \
    (SELECT id FROM t_datasets WHERE date_trunc('month', ts) = '$i-$j-01') AS d \
    LEFT JOIN t_origins AS o ON d.id = o.dataset_id limit 10;" bgp.origins.rv_eqix
  done
done
