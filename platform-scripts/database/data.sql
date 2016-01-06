CREATE TABLE learneractivitysummary (
	leaner_id uuid, 
	m_time_spent double, 
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
	PRIMARY KEY (leaner_id)
);