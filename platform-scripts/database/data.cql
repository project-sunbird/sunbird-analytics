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

DROP TABLE IF EXISTS learner_db.devicespecification;
DROP TABLE IF EXISTS learner_db.device_usage_summary;
DROP TABLE IF EXISTS learner_db.device_content_summary_fact;

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
    d_content_id text,
    d_period Int,
    d_group_user boolean,
    d_content_type text,
    d_mime_type text,
    m_publish_date timestamp,
    m_last_sync_date timestamp,
    m_total_ts double,
    m_total_sessions bigint,
    m_avg_ts_session double,
    m_total_interactions bigint,
    m_avg_interactions_min double,
    m_avg_sessions_week double,
    m_avg_ts_week double,
    PRIMARY KEY (d_content_id, d_period, d_group_user)
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

insert into device_db.device_recos (device_id, scores) values ('5edf49c4-313c-4f57-fd52-9bfe35e3b7d6', [('domain_14443', 0.04355823283156912), ('domain_14433', 0.8409154650037736), ('domain_63844', 0.35321659380730264), ('domain_46963', 0.20968588627301266), ('domain_38245', 0.05234363040235501), ('domain_14426', 0.8567234280604059), ('org.ekstep.moreless.worksheet', 0.438426513039726), ('org.ekstep.ms_534d17eb69702d6bce040000', 0.972410379557415), ('domain_68601', 0.17960970274817534), ('domain_68491', 0.8680747675828835), ('domain_14666', 0.7914352581759874), ('org.ekstep.story.hi.vayu', 0.11741330484826329), ('org.ekstep.story.hi.tree', 0.2755097187093627), ('org.ekstep.Thetreehindi', 0.6449610595651174), ('org.ekstep.Vayuthewindhindi', 0.41085693618939767), ('org.ekstep.time.worksheet', 0.10667712586576239), ('domain_57028', 0.3921280268146796), ('org.ekstep.mar8.story', 0.48481444138130736), ('org.ekstep.apr19.story', 0.7844634549616152), ('domain_37974', 0.5694845021748547), ('org.ekstep.akshara1.worksheet', 0.24075131703087016), ('org.ekstep.ms_53aab12a69702d0da4050000', 0.22918992652808412), ('domain_56005', 0.02303874361839031), ('domain_56080', 0.3078902422199603), ('domain_56021', 0.4649065060677675), ('domain_15277', 0.5080414821820678), ('org.ekstep.story.hi.joy', 0.885586875682727), ('org.ekstep.story.jhoola', 0.8296389895591669), ('org.ekstep.khusi', 0.9797759970293374), ('domain_57000', 0.4488072028699466), ('domain_57015', 0.8440119777943664), ('domain_57064', 0.26215152416900267), ('domain_14625', 0.07483659375553775), ('org.ekstep.hindi.num.activity', 0.33132654351526303), ('org.ekstep.hindi.lit.activities', 0.7044278609738144), ('org.ekstep.ms_534d0c1f69702d5ffe030000', 0.37982308805023024), ('org.ekstep.story.hi.wedding', 0.08720163632552413), ('org.ekstep.unclesweddinghindi', 0.457173615795188), ('org.ekstep.story.hi.elephant', 0.19440556395044828), ('org.ekstep.story.hi.moon', 0.12024916434121558), ('org.ekstep.moonandthecaphindi', 0.44933193464586374), ('domain_44986', 0.6695033629514398), ('org.ekstep.story.hi.colours', 0.6888908700497187), ('org.ekstep.story.hi.happy', 0.9045271446821523), ('org.ekstep.story.hi.sweet', 0.7609140313245555), ('org.ekstep.story.hi.colour', 0.25590760368543797), ('domain_68635', 0.020840459656513044), ('domain_56879', 0.952937207675407), ('domain_14589', 0.9501753923169671), ('org.ekstep.tellmenowhappyandsadhindi', 0.110911828656447), ('org.ekstep.tellmenowcolourshindi', 0.18021607033649145), ('org.ekstep.tellmenowsweetandsourhindi', 0.9644501193800803), ('org.ekstep.story.ka.elephant', 0.7953110925426563), ('org.ekstep.story.hi.listen', 0.6636429215832487), ('org.ekstep.Listentomybodyhindi', 0.020120279831549626), ('domain_15300', 0.19754371884832023), ('org.ekstep.counting.worksheet', 0.6414965923699831), ('domain_56990', 0.7999089479783097), ('org.ekstep.story.hi.nature', 0.16182075351042058), ('domain_46039', 0.45468573170299686), ('org.ekstep.coloursofnaturehindi', 0.3958272829359877), ('domain_46964', 0.3343820412218754), ('domain_13483', 0.6463383053914968), ('org.ekstep.ms_53aaaf0169702d0c2e030000', 0.9745563364723717), ('domain_15285', 0.2999969812894392), ('domain_15287', 0.09817287231416616), ('org.ekstep.addobj.worksheet', 0.8836532657894598), ('domain_38239', 0.5848868112705221), ('org.ekstep.numchart.worksheet', 0.18470076871158225), ('domain_46665', 0.4332158475950363), ('domain_47659', 0.3772342955520107), ('org.ekstep.hindistorypart2', 0.9037658907331156), ('org.ekstep.prathamhindi1', 0.33836867788998903), ('org.ekstep.prathamhindi2', 0.5875348336541124), ('org.ekstep.hindistorypart3', 0.6840264422847473), ('org.ekstep.prathamhindi3', 0.5092544984213729), ('org.ekstep.hindistorypart4', 0.40923671849586785), ('org.ekstep.prathamhindi4', 0.1184199121158207), ('org.ekstep.hindistorypart5', 0.4145800233784227), ('org.ekstep.prathamhindi5', 0.5823595209903936), ('org.ekstep.hindistorypart6', 0.7218982645520415), ('org.ekstep.prathamhindi6', 0.6469036971723319), ('domain_47087', 0.664459138827076), ('org.ekstep.hindi.stories', 0.6507851630748551), ('domain_51435', 0.10511948406480176), ('domain_51548', 0.6302602391285814), ('domain_17221', 0.9892200882763045), ('domain_47563', 0.14859845854121811), ('domain_49024', 0.4363037673860586), ('domain_63382', 0.5740885191572962), ('org.ekstep.ms_52fdbc1e69702d16cd040000', 0.5468389258754274), ('test.org.ekstep.action-chaining', 0.8424172244148165), ('domain_66672', 0.6979675769170413), ('do_10093911', 0.13534776381695557), ('domain_59928', 0.7146274121381258), ('domain_48905', 0.21611629653884323), ('domain_57434', 0.03550588134983057), ('domain_60598', 0.40496952614674186), ('domain_59927', 0.9496081242149996), ('domain_66036', 0.8713616793295309)]);