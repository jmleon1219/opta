drop procedure upsert_event;
create or replace procedure upsert_event(in p_audit_id audit.audit_id%TYPE, in p_audit_tms audit.start_tms%type, in p_file_name audit.file_name%type, in p_file_tms audit.file_tms%type)
	language plpgsql 
	as $$ 		

	begin

				with ed as (
							select  distinct
									e.event_id 
							 from stg.f24_parsed_stg fps
					   inner join event e
					   		   on fps.e_uid = e.opta_event_uid
					   	    where fps.file_name = p_file_name
					   	      and fps.file_timestamp = p_file_tms
					   )
				delete
				  from qualifier q
				 using ed e
				 where q.event_id = e.event_id;
				
				
				delete
				  from qualifier q
				 using stg.f24_parsed_stg f
			     where q.opta_qualifier_uid = f.q_uid
		           and f.file_name = p_file_name
		           and f.file_timestamp = p_file_tms;
/*
			 delete
			   from qualifier q
			  using stg.f24_parsed_stg f,
			  		event e
			  where f.e_uid = e.opta_event_uid 
			    and f.e_uid = q.opta_qualifier_uid 
			    and f.file_name = p_file_name
			    and f.file_timestamp = p_file_tms; 		

			 delete
			   from qualifier q
			  using stg.f24_parsed_stg f
		      where q.opta_qualifier_uid = f.q_uid
		        and f.file_name = p_file_name
			    and f.file_timestamp = p_file_tms;
*/
		     delete
			   from event e
			  using stg.f24_parsed_stg f24
			  where e.opta_event_uid = f24.e_uid
		        and f24.file_name = p_file_name
		        and f24.file_timestamp = p_file_tms;
		       
		       
		     delete
		       from stg.orphan_foreign_key o 
		      using stg.f24_parsed_stg  f
		      where o.pk_value = f.e_uid
		        and o."table" = 'event'
		        --and f24.file_name = p_file_name
		        --and f24.file_timestamp = p_file_tms
		       ;
		       
			     
				with nr as 
				(
						  		select distinct
						  			   typ.code_id,
						  			   fps."version",
						  			   fps.x,
						  			   fps.y,
						  			   fps.outcome,
						  			   fps.min,
						  			   fps.sec,
						  			   fps.period_id,
						  			   t.team_id as team_id,
						  			   p.player_id as player_id,
						  			   m.match_id,
						  			   fps.e_uid,
						  			   fps.event_id
						  		  from stg.f24_parsed_stg fps 
						  	 left join code typ
						  	 		on cast(fps.type_id as int) = typ.source_id 
						  	 	   and typ."type" = 'event_type'
						  	 left join team t
						  	 		on 't' || fps.team_id = t.opta_team_uid 
						  	 left join player p 
						  	 		on 'p' || fps.player_id = p.opta_player_uid 
						  	 left join match m 
						  	 		on 'g' || fps.g_uid = m.opta_match_uid
						  	    where fps.file_name = p_file_name
						  	      and fps.file_timestamp = p_file_tms
				)
				insert into event(type_id, "version", x, y, outcome, min, sec, period_id, event_team_id, event_player_id, match_id, opta_event_uid, opta_event_id, insert_tms, insert_audit_id)
						select code_id, "version", x, y, outcome, min, sec, period_id, team_id, player_id, match_id, e_uid, event_id, p_audit_tms, p_audit_id
								from nr;
			
			
				with nr as
				(
				  	 
					select   'event' 	as "table",
							 'team_id' 	as "column",
							  't' || fps.team_id as "raw_value",
							  fps.e_uid as "pk_value"
					      from stg.f24_parsed_stg fps
					 left join team t
		 				    on 't' || fps.team_id = t.opta_team_uid
		 				 where t.team_id is null
		 				   and fps.team_id is not null
		 				   and fps.file_name = p_file_name
			      		   and fps.file_timestamp = p_file_tms
					union
						select  'event' 	as "table",
								'player_id' as "column",
								 'p' || fps.player_id as "raw_value",
								 fps.e_uid as "pk_value"
						  from stg.f24_parsed_stg fps
				  	 left join player p 
				  	 		on 'p' || fps.player_id = p.opta_player_uid 
			 			 where p.player_id is null
			 			   and fps.player_id is not null
		 				   and fps.file_name = p_file_name
			      		   and fps.file_timestamp = p_file_tms
			 	    union
			 	   		select  'event' 	as "table",
								'match_id' 	as "column",
								 'g' || fps.g_uid as "raw_value",
								 fps.e_uid as "pk_value"
						  from stg.f24_parsed_stg fps
				  	 left join match m 
				  	 		on 'g' || fps.g_uid = m.opta_match_uid
			 			 where m.match_id is null
			 			   and fps.g_uid is not null
			 			   and fps.file_name = p_file_name
			      		   and fps.file_timestamp = p_file_tms
		 		)
			    insert into stg.orphan_foreign_key("table", "column", raw_value, pk_value)
			    		select "table", "column", raw_value, pk_value from nr
			    on conflict on constraint orphan_foreign_key_unq
			    		do update set raw_value = excluded.raw_value;
			    	
			    	
			   commit;
	    	
	   end;
		$$;
