query_sum_pfxlen <- paste("SELECT masklen(p.prefix), count(masklen(p.prefix)) FROM",
                          "((SELECT prefix_id FROM t_origins WHERE dataset_id = 1) AS o",
                          "LEFT JOIN t_prefixes p ON o.prefix_id = p.id) group by masklen",sep=" ")

query_avg_pfxlen <- paste("SELECT avg(masklen(p.prefix)) FROM",
                          "((SELECT prefix_id FROM t_origins WHERE dataset_id = 1) AS o",
                          "LEFT JOIN t_prefixes p ON o.prefix_id = p.id)",sep=" ")