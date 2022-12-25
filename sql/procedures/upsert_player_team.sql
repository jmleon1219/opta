drop procedure upsert_player_team;
create or replace procedure upsert_player_team(in p_audit_id audit.audit_id%TYPE, in p_audit_tms audit.start_tms%type, in p_file_name audit.file_name%type, in p_file_tms audit.file_tms%type)
	language sql
	as $$ 	

				with nr as(
								select p.player_id,
									   t.team_id,
									   case when loan_ind = 1 then true else false end as on_loan,
									   ps.join_date,
									   ps.jersey_num,
									   ps.real_position,
									   ps.real_position_side,
									   ps.weight,
									   ps.height,
									   ps.preferred_foot,
									   ps.season_name,
									   ps.season_id 
								  from stg.player_stg ps
						    inner join team t
						    		on ps.t_id = t.opta_team_uid
							inner join player p 
									on ps.p_id  = p.opta_player_uid
								 where ps.file_name = p_file_name
								   and ps.file_timestamp = p_file_tms									
							except
								select player_id,
									   team_id,
									   on_loan,
									   join_date,
									   jersey_num,
									   position,
									   position_side,
									   weight,
									   height,
									   preferred_foot,
									   season_name,
									   opta_season_id
								  from player_team
							)
				insert into player_team(player_id, team_id, on_loan, join_date, jersey_num, "position", position_side, weight, height, preferred_foot, season_name, opta_season_id, insert_tms, insert_audit_id)
						select player_id, team_id, on_loan, join_date, jersey_num, real_position, real_position_side, weight, height, preferred_foot, season_name, season_id, p_audit_tms, p_audit_id
						  from nr
				on conflict on constraint player_team_season_unq
				do update set on_loan = excluded.on_loan, join_date = excluded.join_date, jersey_num = excluded.jersey_num,  "position" = excluded.position, position_side = excluded.position_side, weight = excluded.weight, "height" = excluded.height, 
								preferred_foot = excluded.preferred_foot, season_name = excluded.season_name, update_tms = p_audit_tms, update_audit_id = p_audit_id;
	
	$$;
