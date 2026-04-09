PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE nodes (
	id INTEGER NOT NULL,
	"key" VARCHAR(1023) NOT NULL,
	ancestors JSON,
	structure_family VARCHAR(9) NOT NULL,
	metadata JSON NOT NULL,
	specs JSON NOT NULL,
	time_created DATETIME DEFAULT (CURRENT_TIMESTAMP),
	time_updated DATETIME DEFAULT (CURRENT_TIMESTAMP),
	PRIMARY KEY (id),
	CONSTRAINT key_ancestors_unique_constraint UNIQUE ("key", ancestors)
);
INSERT INTO nodes VALUES(1,'x','[]','array','{"color":"blue"}','[]','2024-05-25 10:18:38','2024-05-25 10:18:38');
CREATE TABLE structures (
	id VARCHAR(32) NOT NULL,
	structure JSON NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (id)
);
INSERT INTO structures VALUES('8e5b0a1237f27c3d04d2cb94bc695ff8','{"data_type":{"endianness":"little","kind":"i","itemsize":8},"chunks":[[3]],"shape":[3],"dims":null,"resizable":false}');
CREATE TABLE assets (
	id INTEGER NOT NULL,
	data_uri VARCHAR(1023),
	is_directory BOOLEAN NOT NULL,
	hash_type VARCHAR(63),
	hash_content VARCHAR(1023),
	size INTEGER,
	time_created DATETIME DEFAULT (CURRENT_TIMESTAMP),
	time_updated DATETIME DEFAULT (CURRENT_TIMESTAMP),
	PRIMARY KEY (id)
);
INSERT INTO assets VALUES(1,'file://localhost/home/dallan/Repos/bnl/tiled/data/x',1,NULL,NULL,NULL,'2024-05-25 10:18:38','2024-05-25 10:18:38');
CREATE TABLE data_sources (
	id INTEGER NOT NULL,
	node_id INTEGER NOT NULL,
	structure_id VARCHAR(32),
	mimetype VARCHAR(255) NOT NULL,
	parameters JSON,
	management VARCHAR(9) NOT NULL,
	structure_family VARCHAR(9) NOT NULL,
	time_created DATETIME DEFAULT (CURRENT_TIMESTAMP),
	time_updated DATETIME DEFAULT (CURRENT_TIMESTAMP),
	PRIMARY KEY (id),
	FOREIGN KEY(node_id) REFERENCES nodes (id) ON DELETE CASCADE,
	FOREIGN KEY(structure_id) REFERENCES structures (id) ON DELETE CASCADE
);
INSERT INTO data_sources VALUES(1,1,'8e5b0a1237f27c3d04d2cb94bc695ff8','application/x-zarr','{}','writable','array','2024-05-25 10:18:38','2024-05-25 10:18:38');
CREATE TABLE revisions (
	id INTEGER NOT NULL,
	node_id INTEGER NOT NULL,
	revision_number INTEGER NOT NULL,
	metadata JSON NOT NULL,
	specs JSON NOT NULL,
	time_created DATETIME DEFAULT (CURRENT_TIMESTAMP),
	time_updated DATETIME DEFAULT (CURRENT_TIMESTAMP),
	PRIMARY KEY (id),
	CONSTRAINT node_id_revision_number_unique_constraint UNIQUE (node_id, revision_number),
	FOREIGN KEY(node_id) REFERENCES nodes (id) ON DELETE CASCADE
);
CREATE TABLE data_source_asset_association (
	data_source_id INTEGER NOT NULL,
	asset_id INTEGER NOT NULL,
	parameter VARCHAR(255),
	num INTEGER,
	PRIMARY KEY (data_source_id, asset_id),
	CONSTRAINT parameter_num_unique_constraint UNIQUE (data_source_id, parameter, num),
	FOREIGN KEY(data_source_id) REFERENCES data_sources (id) ON DELETE CASCADE,
	FOREIGN KEY(asset_id) REFERENCES assets (id) ON DELETE CASCADE
);
INSERT INTO data_source_asset_association VALUES(1,1,'data_uri',NULL);
CREATE TABLE alembic_version (
	version_num VARCHAR(32) NOT NULL,
	CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
INSERT INTO alembic_version VALUES('e756b9381c14');
CREATE INDEX ix_nodes_id ON nodes (id);
CREATE INDEX top_level_metadata ON nodes (ancestors, time_created, id, metadata);
CREATE UNIQUE INDEX ix_assets_data_uri ON assets (data_uri);
CREATE INDEX ix_assets_id ON assets (id);
CREATE INDEX ix_data_sources_id ON data_sources (id);
CREATE INDEX ix_revisions_id ON revisions (id);
CREATE TRIGGER cannot_insert_num_null_if_num_exists
BEFORE INSERT ON data_source_asset_association
WHEN NEW.num IS NULL
BEGIN
    SELECT RAISE(ABORT, 'Can only insert num=NULL if no other row exists for the same parameter')
    WHERE EXISTS
    (
        SELECT 1
        FROM data_source_asset_association
        WHERE parameter = NEW.parameter
        AND data_source_id = NEW.data_source_id
    );
END;
CREATE TRIGGER cannot_insert_num_int_if_num_null_exists
BEFORE INSERT ON data_source_asset_association
WHEN NEW.num IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'Can only insert INTEGER num if no NULL row exists for the same parameter')
    WHERE EXISTS
    (
        SELECT 1
        FROM data_source_asset_association
        WHERE parameter = NEW.parameter
        AND num IS NULL
        AND data_source_id = NEW.data_source_id
    );
END;
COMMIT;
