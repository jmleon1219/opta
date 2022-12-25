drop procedure upsert_player;
create or replace procedure upsert_player(in p_audit_id audit.audit_id%TYPE, in p_audit_tms audit.start_tms%type, in p_file_name audit.file_name%type, in p_file_tms audit.file_tms%type)
	language sql
	as $$ 	

			with nr as (
			 			select  distinct
			 					first_name,
			 					last_name,
			 					middle_name,
			 					known_name,
			 					birth_date,
			 					birth_place,
			 					nat.country_id as natly_country_id,
			 					c.country_id,
			 					deceased,
			 					p_id
			 			  from stg.player_stg ps
			 		 left join country c
			 				on ps.player_country = c."name" 
			 		 left join country nat
			 				on ps.first_nationality = nat.name
			 			 where ps.file_name = p_file_name
			 			   and ps.file_timestamp = p_file_tms
			 		except
			 			select first_name, 
			 				   middle_name, 
			 				   last_name, 
			 				   known_name, 
			 				   birth_date, 
			 				   birth_city, 
			 				   first_natlty_country_id,
			 				   country_id,
			 				   deceased,
			 				   opta_player_uid
			 		    from player			
			)
			insert into player(first_name, middle_name, last_name, known_name, birth_date, birth_city, first_natlty_country_id, country_id, deceased, opta_player_uid, insert_tms, insert_audit_id)
						 select first_name,	middle_name, last_name, known_name,	birth_date,	birth_place, natly_country_id, country_id, deceased, p_id, p_audit_tms, p_audit_id
						   from nr
			on conflict on constraint opta_player_uid_unq
			do update set first_name = excluded.first_name, middle_name = excluded.middle_name, last_name = excluded.last_name, known_name = excluded.known_name, birth_date = excluded.birth_date, 
						  birth_city = excluded.birth_city, first_natlty_country_id = excluded.first_natlty_country_id, country_id = excluded.country_id, deceased = excluded.deceased, 
						  update_tms = p_audit_tms, update_audit_id = p_audit_id;
			$$;
