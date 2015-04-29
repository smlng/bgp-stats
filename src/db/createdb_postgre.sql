-- -----------------------
-- table datasets 
-- -----------------------
DROP TABLE IF EXISTS t_datasets;

CREATE TABLE IF NOT EXISTS t_datasets (
    id      	SERIAL PRIMARY KEY,
    ts   		timestamp without time zone NOT NULL,
    maptype 	TEXT NOT NULL,
    subtype 	TEXT NOT NULL
);

-- -----------------------
-- table prefixes
-- -----------------------
DROP TABLE IF EXISTS t_prefixes;

CREATE TABLE IF NOT EXISTS t_prefixes (
	id			SERIAL PRIMARY KEY,
	prefix  	INET NOT NULL
);

-- -----------------------
-- table origins
-- -----------------------
DROP TABLE IF EXISTS t_origins;

CREATE TABLE IF NOT EXISTS t_origins (
	dataset_id	INT,
	prefix_id  	INT,
	asn			INT,
	PRIMARY KEY (dataset_id, prefix_id, asn),
	FOREIGN KEY (dataset_id) REFERENCES t_datasets (id),
	FOREIGN KEY (prefix_id) REFERENCES t_prefixes (id)
);