CREATE KEYSPACE learner_db WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TABLE learnersnapshot (
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

CREATE TABLE learnerproficiency(
	learner_id text,
	proficiency map<text,double>,
	start_time timestamp,
	end_time timestamp,
	model_params map<text,text>,
	PRIMARY KEY (learner_id)
);

CREATE TABLE learnercontentsummary(
	learner_id text,
	content_id text,
	time_spent double,
	interactions_per_min double,
	num_of_sessions_played int,
	PRIMARY KEY (learner_id,content_id)
);

CREATE TABLE conceptrelevance(
	learner_id text,
	concept_id text,
	relevance double,
	PRIMARY KEY (learner_id)
);