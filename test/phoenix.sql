
-- Phoenix limitations:
-- * Non-null column must be part of primary key
-- * Does not support upsert with multiple rows in one statement
-- * Does not support stmt.getGeneratedKeys
-- * Does not support Stateful DEFAULT value, see #PHOENIX-3425

CREATE SEQUENCE IF NOT EXISTS user_id_seq;

DROP TABLE IF EXISTS user;
CREATE TABLE IF NOT EXISTS user (
  id          BIGINT NOT NULL,
  username    VARCHAR(64),
  email       VARCHAR(64),
  phonenumber VARCHAR(16),
  created_at  TIME DEFAULT '2017-01-01',
  CONSTRAINT pk PRIMARY KEY (id)
);

CREATE SEQUENCE IF NOT EXISTS address_id_seq;
DROP TABLE IF EXISTS address;
CREATE TABLE address (
  id      BIGINT NOT NULL,
  user_id INTEGER,
  country CHAR(2),
  state   VARCHAR(16),
  city    VARCHAR(64),
  zipcode VARCHAR(8),
  line    VARCHAR(64),
  created_at TIME DEFAULT '2017-01-01',
  CONSTRAINT pk PRIMARY KEY (id)
);
