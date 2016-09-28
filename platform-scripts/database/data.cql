CREATE KEYSPACE IF NOT EXISTS learner_db WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS learner_db.learnersnapshot (
	learner_id text, 
	m_time_spent double, 
	m_time_btw_gp double,
	m_active_time_on_pf double, 
	m_interrupt_time double, 
	t_ts_on_pf double,
	m_ts_on_an_act map<text,double>,
	m_count_on_an_act map<text,double>,
	n_of_sess_on_pf int,
	l_visit_ts timestamp,
	most_active_hr_of_the_day int,
	top_k_content list<text>,
	sess_start_time timestamp,
	sess_end_time timestamp,
	dp_start_time timestamp,
	dp_end_time timestamp,
	PRIMARY KEY (learner_id)
);

CREATE TABLE IF NOT EXISTS learner_db.learnerproficiency(
	learner_id text,
	proficiency map<text,double>,
	start_time timestamp,
	end_time timestamp,
	model_params map<text,text>,
	PRIMARY KEY (learner_id)
);

CREATE TABLE IF NOT EXISTS learner_db.learnercontentsummary(
	learner_id text,
	content_id text,
	time_spent double,
	interactions_per_min double,
	num_of_sessions_played int,
	PRIMARY KEY (learner_id,content_id)
);

CREATE TABLE IF NOT EXISTS learner_db.learnerconceptrelevance(
	learner_id text,
	relevance map<text,double>,
	PRIMARY KEY (learner_id)
);

CREATE TABLE IF NOT EXISTS learner_db.conceptsimilaritymatrix (
	concept1 text,
	concept2 text,
	relation_type text,
	sim double,
	PRIMARY KEY (concept1, concept2)
);

CREATE TABLE IF NOT EXISTS learner_db.learnerprofile (
	learner_id text,
	did text,
	gender text,
	language text,
	loc text,
	standard int,
	age int,
	year_of_birth int,
	group_user boolean,
	anonymous_user boolean,
	created_date timestamp,
	updated_date timestamp,
	PRIMARY KEY (learner_id)
);

CREATE KEYSPACE IF NOT EXISTS content_db WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS content_db.content_cumulative_summary (
	content_id text, 
	start_date timestamp, 
	total_ts double,
	total_num_sessions bigint, 
	average_ts_session double, 
	total_interactions bigint,
	average_interactions_min double,
	num_sessions_week double,
	ts_week double,
	content_type text,
	mime_type text,
	PRIMARY KEY (content_id)
);

CREATE TABLE IF NOT EXISTS content_db.content_usage_metrics (
	id text, 
	top_k_timespent map<text,double>,
	top_k_sessions map<text,bigint>,
	PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS content_db.content_usage_summary_fact (
    d_period Int,
    d_tag text,
    d_content_id text,
    m_publish_date timestamp,
    m_last_sync_date timestamp,
    m_last_gen_date timestamp,
    m_total_ts double,
    m_total_devices bigint,
    m_device_ids blob,
    m_total_sessions bigint,
    m_avg_sess_device double,
    m_avg_ts_session double,
    m_total_interactions bigint,
    m_avg_interactions_min double,
    PRIMARY KEY (d_period, d_tag, d_content_id)
);

CREATE TABLE IF NOT EXISTS content_db.content_popularity_summary_fact (
	d_period int,
	d_tag text,
	d_content_id text,
	m_downloads bigint,
	m_side_loads bigint,
	m_comments list<frozen <tuple<text, timestamp>>>,
	m_ratings list<frozen <tuple<double, timestamp>>>,
	m_avg_rating double,
	PRIMARY KEY (d_period, d_tag, d_content_id)
);

CREATE TABLE IF NOT EXISTS content_db.item_usage_summary_fact (
	d_period int,
	d_tag text,
	d_content_id text,
	d_item_id text,
	m_total_ts double,
	m_total_count int,
	m_correct_res_count int,
	m_inc_res_count int,
	m_correct_res list<text>,
	m_top5_incorrect_res list<text>,
	m_avg_ts double,
	PRIMARY KEY (d_period, d_tag, d_content_id)
);

CREATE TABLE IF NOT EXISTS content_db.genie_launch_summary_fact (
	d_period int,
	d_tag text,
	m_total_sessions bigint,
	m_total_ts double,
	m_total_devices bigint,
	m_device_ids blob,
	m_avg_sess_device bigint,
	m_avg_ts_session double,
	m_contents list<text>,
	PRIMARY KEY (d_period, d_tag)
);

CREATE TABLE IF NOT EXISTS content_db.content_sideloading_summary (
    content_id text,
    num_downloads bigint,
    total_count bigint,
    num_sideloads bigint,
    origin_map map<text,double>,
    avg_depth double,
    PRIMARY KEY (content_id)
);

CREATE TABLE IF NOT EXISTS content_db.content_to_vector(
	content_id text,
	text_vec list<double>,
	tag_vec list<double>,
	PRIMARY KEY (content_id)
);

CREATE TABLE IF NOT EXISTS content_db.registered_tags(
	tag_id text,
	last_updated timestamp,
	active boolean,
	PRIMARY KEY (tag_id)
);

CREATE KEYSPACE IF NOT EXISTS device_db WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE IF NOT EXISTS device_db.device_recos(
	device_id text,
	scores list<frozen <tuple<text, double>>>,
	PRIMARY KEY (device_id)
);

CREATE TABLE IF NOT EXISTS device_db.device_specification (
	device_id text, 
	device_name text, 
	device_local_name text,
	os text, 
	make text, 
	memory double,
	internal_disk double,
	external_disk double,
	screen_size double,
	primary_secondary_camera text,
	cpu text,
	num_sims double,
	capabilities list<text>,
	PRIMARY KEY (device_id,os,screen_size)
);

CREATE TABLE IF NOT EXISTS device_db.device_usage_summary (
    device_id text,
    start_time timestamp,
    end_time timestamp,
    num_days bigint,
    total_launches bigint,
    total_timespent double,
    avg_num_launches double,
    avg_time double,
    num_contents bigint,
    play_start_time timestamp,
    last_played_on timestamp,
    total_play_time double,
    num_sessions bigint,
    mean_play_time double,
    mean_play_time_interval double,
    last_played_content text,
    PRIMARY KEY (device_id)
);

CREATE TABLE IF NOT EXISTS device_db.device_content_summary_fact (
    device_id text,
    content_id text,
    game_ver text,
    num_sessions bigint,
    total_interactions bigint, 
    avg_interactions_min double,
    total_timespent double,
    last_played_on timestamp,
    start_time timestamp,
    mean_play_time_interval double,
    downloaded boolean,
    download_date timestamp,
    num_group_user bigint,
    num_individual_user bigint,
    PRIMARY KEY (device_id, content_id)
);
