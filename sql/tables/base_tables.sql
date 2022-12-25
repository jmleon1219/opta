drop table country cascade;  
create table country
(
	country_id			serial primary key,
	name				varchar(100),
	code				varchar(2),
	update_tms			timestamp,
	insert_tms			timestamp default current_timestamp,
	update_audit_id		int,
	insert_audit_id		int,
	constraint name_unq unique(name)
);
grant select on public.country to group dev;
  


drop table competition cascade;
create table competition
(
	competition_id				serial primary key,
	code						varchar(10),
	name						varchar(100),
	number						int,
	season						int,
	season_name					varchar(25),
	update_tms					timestamp,
	insert_tms					timestamp default current_timestamp,
	update_audit_id				int,
	insert_audit_id				int,
	constraint code_season_unq unique(code, season)
);
create index opta_comp_num_nun01 on competition("number");
grant select on public.competition to group dev;


drop table team cascade;  
create table team
(
	team_id						serial primary key,
	official_name				varchar(100),
	name						text,
	short_name					varchar(50),
	symid						varchar(10),
	region						varchar(25),
	founded_year				int,
	country_id					int references country(country_id),
	opta_team_uid				varchar(25),
	update_tms					timestamp,
	insert_tms					timestamp default current_timestamp,
	update_audit_id				int,
	insert_audit_id				int,
	constraint opta_team_uid_unq unique(opta_team_uid)
);
grant select on public.team to group dev;
--alter table team alter column short_name type varchar(50);



drop table player cascade;
create table player
(
	player_id					serial primary key,
	first_name					varchar(50),
	middle_name					varchar(50),
	last_name					varchar(50),
	known_name					varchar(50),
	birth_date					date,
	birth_city					varchar(50),
	first_natlty_country_id		int references country(country_id),
	country_id					int references country(country_id),
	deceased					varchar(25),
	opta_player_uid				varchar(25),
	update_tms					timestamp,
	insert_tms					timestamp default current_timestamp,
	update_audit_id				int,
	insert_audit_id				int,
	constraint opta_player_uid_unq unique(opta_player_uid)
);
grant select on public.player to group dev;


drop table player_team cascade;
create table player_team
(
	player_id					int references player(player_id),
	team_id						int references team(team_id),
	on_loan						boolean,
	join_date					date,
	jersey_num					varchar(25),
	position					varchar(50),
	position_side				varchar(50),
	weight						int,
	height						int,
	preferred_foot				varchar(25),
	season_name					varchar(25),
	opta_season_id				int,
	update_tms					timestamp,
	insert_tms					timestamp default current_timestamp,
	update_audit_id				int,
	insert_audit_id				int,
	constraint player_team_season_unq unique(player_id, team_id, opta_season_id)
);
grant select on public.player_team to group dev;


drop table match cascade;
create table "match"
(
	match_id					serial primary key,
	"day"						int,
	"type"						varchar(25),
	home_team_id				int references team(team_id),
	away_team_id				int references team(team_id),
	winner_team_id				int references team(team_id),
	"period"					varchar(25),
	date						timestamp without time zone,
	timezone					varchar(3),
	venue						varchar(200),
	city						varchar(200),
	round_type					varchar(200),
	leg							varchar(200),
	timing_type					varchar(50),
	timing_detail_type			varchar(50),
	timestamp_accuracy_type		varchar(100),
	additional_info				varchar(200),
	opta_match_uid				varchar(25),
	competition_id				int references competition(competition_id),
	update_tms					timestamp,
	insert_tms					timestamp default current_timestamp,
	update_audit_id				int,
	insert_audit_id				int,
	constraint opta_match_uid_unq unique(opta_match_uid)
);
grant select on public.match to group dev;
create unique index opta_match_uid_fn_unq01 on match(substring(opta_match_uid, 2, length(opta_match_uid )));

drop table event cascade;
create table event
(
	event_id					bigserial primary key,
	type_id						int references code(code_id),
	version						varchar(50),
	x							decimal,
	y							decimal,
	outcome						int,
	min							int,
	sec							int,
	period_id					varchar(50),
	event_team_id				int references team(team_id),
	event_player_id				int references player(player_id),
	match_id					int references match(match_id),
	opta_event_uid				varchar(25),
	opta_event_id				varchar(25),
	update_tms					timestamp,
	insert_tms					timestamp default current_timestamp,
	update_audit_id				int,
	insert_audit_id				int,
	constraint opta_event_uid_unq unique(opta_event_uid)
);
create index event_event_team_id on event(event_team_id);
create index event_event_player_id on event(event_player_id);
create index event_match_id on event(match_id);
grant select on public.event to group dev;

drop table qualifier;
create table qualifier
(
	qualifier_id 				bigserial primary key,
	qualifier_code_id			int references code(code_id),
	value						text,
	opta_qualifier_uid			varchar(25),
	opta_qualifier_id			varchar(25),
	event_id					bigint references event(event_id),
	update_tms					timestamp,
	insert_tms					timestamp default current_timestamp,
	update_audit_id				int,
	insert_audit_id				int,
	constraint opta_qualifier_uid_unq unique(opta_qualifier_uid)
);
create index qualifier_event_id_nunq01 on qualifier(event_id);
grant select on public.qualifier to group dev;
--alter table qualifier alter column value type text;


create table code
(
	code_id						serial primary key,
	type						varchar(50),
	source_id					int,
	value						varchar(50),
	description					text,
	update_tms					timestamp,
	insert_tms					timestamp default current_timestamp
);

grant select on public.code to group dev;


drop table audit;
create table audit
(
	audit_id					serial primary key,
	status						varchar(25),
	start_tms					timestamp with time zone,
	end_tms						timestamp with time zone,
	file_tms					timestamp,
	file_name					varchar(50),
	message						text
);
grant select on public.audit to group dev;