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
  ttl           INT,
  PRIMARY KEY (origin_ttl_id, prefix_id, asn, ttl),
  FOREIGN KEY (origin_ttl_id) REFERENCES t_origin_ttl (id),
  FOREIGN KEY (prefix_id) REFERENCES t_prefixes (id)
);
