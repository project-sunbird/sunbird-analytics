CREATE KEYSPACE IF NOT EXISTS content_store WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS content_store.content_data (
    content_id text PRIMARY KEY,
    body blob,
    last_updated_on timestamp,
    oldbody blob);