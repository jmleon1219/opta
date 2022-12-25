drop procedure upsert_match;
create or replace procedure upsert_match(in p_audit_id audit.audit_id%TYPE, in p_audit_tms audit.start_tms%type, in p_file_name audit.file_name%TYPE)
	language sql
	as $$ 			
	
		 with nr as (
					   select distinct
					   		  match_day,
					   		  match_type,
					   		  h.team_id as "home_team_id",
					   		  a.team_id as "away_team_id",
					   		  w.team_id as "winner_team_id",
					   		  "period",
					   		  match_date,
					   		  match_tz,
					   		  venue,
					   		  city,
					   		  round_type,
					   		  leg,
					   		  timing_type,
					   		  timing_detail_type,
					   		  timestamp_accuracy_type,
					   		  additional_info,
					   		  ms.g_id,
					   		  c.competition_id
					     from stg.match_stg ms
				    left join team w
						   on ms.match_winner = w.opta_team_uid
				    left join team h
						   on ms.home = h.opta_team_uid						   
				    left join team a
						   on ms.away = a.opta_team_uid						   
				    left join competition c 
				  		   on ms.competition_code  = c.code 
				  		  and ms.season_id  = c.season
				  		where ms.file_name = p_file_name
				  except 
				 select "day", "type", home_team_id, away_team_id, winner_team_id, "period", "date", timezone, venue, city, round_type, leg, timing_type, timing_detail_type, timestamp_accuracy_type, additional_info, opta_match_uid, competition_id
				   from match		 		 
		 )
		 insert into match("day", "type", home_team_id, away_team_id, winner_team_id, "period", "date", timezone, venue, city, round_type, leg, timing_type, timing_detail_type, timestamp_accuracy_type, additional_info, 
		 					opta_match_uid, competition_id, insert_tms, insert_audit_id)
				select match_day, match_type, home_team_id, away_team_id, winner_team_id, "period", match_date, match_tz, venue, city, round_type, leg, timing_type, timing_detail_type, timestamp_accuracy_type, additional_info, 
					   g_id, competition_id, p_audit_tms, p_audit_id	
		  		 from nr
		 on conflict on constraint opta_match_uid_unq
		 do update set "day" = excluded.day, "type" = excluded.type, home_team_id = excluded.home_team_id, away_team_id = excluded.away_team_id, winner_team_id = excluded.winner_team_id, "period" = excluded.period, "date" = excluded.date, timezone = excluded.timezone, venue = excluded.venue,
		 				city = excluded.city, round_type = excluded.round_type, leg = excluded.leg, timing_type = excluded.timing_type, timing_detail_type = excluded.timing_detail_type, 
		 				timestamp_accuracy_type = excluded.timestamp_accuracy_type, additional_info = excluded.additional_info, competition_id = excluded.competition_id, update_tms = p_audit_tms, update_audit_id = p_audit_id;
		$$;