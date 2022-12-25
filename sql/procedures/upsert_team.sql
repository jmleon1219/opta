drop procedure upsert_team;	
create or replace procedure upsert_team(in p_audit_id audit.audit_id%TYPE, in p_audit_tms audit.start_tms%type, in p_file_name audit.file_name%TYPE)
	language sql
	as $$ 	
	
		with nr as(
			 select ts.team_name,
			 		ts.t_id
			   from stg.team_stg ts
			   except
			 select name,
			 		opta_team_uid
			  from team)
		insert into team(name, opta_team_uid, insert_tms, insert_audit_id)
			  select team_name, t_id, p_audit_tms, p_audit_id from nr
		on conflict on constraint opta_team_uid_unq
			do update set name = excluded.name, update_tms = p_audit_tms, update_audit_id = p_audit_id;	
	
	
		 with nr as (
			 	 select distinct
				 		official_club_name,
				 		short_club_name,
				 		ps.team_name, 
				 		symid,
				 		c.country_id,
				 		region_name,
				 		team_founded,
				 		ps.t_id 
				   from stg.player_stg ps
			  left join country c 
			  		on  ps.country = c."name" 
				  where ps.file_name = p_file_name			   
				 except 
				 select official_name, 
						short_name,
						name,
						symid,
						country_id,
						region, 
						founded_year,
						opta_team_uid
				   from team
		 )
	 	 insert into team(official_name, short_name, name, symid, country_id, region, founded_year, opta_team_uid, insert_tms, insert_audit_id)
	 	 		select official_club_name, short_club_name, team_name, symid, country_id, region_name, team_founded, t_id, p_audit_tms, p_audit_id 
	 	 		 from nr
	 	 on conflict on constraint opta_team_uid_unq
	 	 		do update set official_name = excluded.official_name, short_name = excluded.short_name, country_id = excluded.country_id, name = excluded.name, symid = excluded.symid,
	 	 						region = excluded.region, founded_year = excluded.founded_year, update_tms = p_audit_tms, update_audit_id = p_audit_id;
		 

			  
		$$;
