-- -----------------------
-- table datasets
-- -----------------------
CREATE TABLE IF NOT EXISTS t_datasets (
  id          SERIAL PRIMARY KEY,
  ts          timestamp without time zone NOT NULL,
  maptype     TEXT NOT NULL,
  subtype     TEXT NOT NULL
);

-- -----------------------
-- table prefixes
-- -----------------------
CREATE TABLE IF NOT EXISTS t_prefixes (
  id          SERIAL PRIMARY KEY,
  prefix      INET NOT NULL
);

-- -----------------------
-- table origins
-- -----------------------
CREATE TABLE IF NOT EXISTS t_origins (
  dataset_id  INT,
  prefix_id   INT,
  asn         INT,
  PRIMARY KEY (dataset_id, prefix_id, asn),
  FOREIGN KEY (dataset_id) REFERENCES t_datasets (id),
  FOREIGN KEY (prefix_id) REFERENCES t_prefixes (id)
);

-- to optimize performance and vacuum process use partition
-- create partition table per year_month, as follows:
-- CREATE TABLE IF NOT EXISTS t_origins_yyyy_mm () INHERITS (t_origins);

-- -----------------------
-- table origin ttl
-- -----------------------
CREATE TABLE IF NOT EXISTS t_origin_ttl (
  id          SERIAL PRIMARY KEY,
  ts_begin    timestamp without time zone NOT NULL,
  ts_until    timestamp without time zone NOT NULL,
  maptype     TEXT NOT NULL,
  subtype     TEXT NOT NULL
);

-- -----------------------
-- table origin ttl data
-- -----------------------
CREATE TABLE IF NOT EXISTS t_origin_ttl_data (
  origin_ttl_id INT,
  prefix_id     INT,
  asn           INT,
  ts0           timestamp without time zone NOT NULL,
  ts1           timestamp without time zone NOT NULL,
  ttl           INT,
  PRIMARY KEY (origin_ttl_id, prefix_id, asn, ts0, ts1, ttl),
  FOREIGN KEY (origin_ttl_id) REFERENCES t_origin_ttl (id),
  FOREIGN KEY (prefix_id) REFERENCES t_prefixes (id)
);

-- -----------------------
-- table origin diffs
-- -----------------------
CREATE TABLE IF NOT EXISTS t_origin_diffs (
  dataset_id0   INT,
  dataset_id1   INT,
  prefix_new    INT,
  prefix_del    INT,
  prefix_mod    INT,
  FOREIGN KEY (dataset_id0) REFERENCES t_datasets (id),
  FOREIGN KEY (dataset_id1) REFERENCES t_datasets (id),
  PRIMARY KEY (dataset_id0, dataset_id1)
);

-- -----------------------
-- table origin stats
-- -----------------------
CREATE TABLE IF NOT EXISTS t_origin_stats (
  dataset_id    INT,
  FOREIGN KEY (dataset_id) REFERENCES t_datasets (id),
  PRIMARY KEY (dataset_id0)
);
