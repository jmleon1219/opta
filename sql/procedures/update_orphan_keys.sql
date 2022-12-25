create or replace procedure update_orphan_keys(inout p_audit_id int)
 	language plpgsql
 	as $$
 	
 		declare

 				v_audit_id		audit.audit_id%type;
				v_process_tms	timestamp;
				
		  begin
					
				v_process_tms := current_timestamp;
		 		
				insert 
				  into audit(start_tms, status, file_name)
				values (v_process_tms, 'running', 'orphan key reconciliation') 
			 returning audit_id
				  into v_audit_id;
	 	 		
	 		update event e
				   set match_id = m.match_id, update_tms = current_timestamp, update_audit_id = v_audit_id
				  from stg.orphan_foreign_key fk, 
				  		"match" m
				 where fk.raw_value = m.opta_match_uid 
				   and e.opta_event_uid = fk.pk_value
			       and "table" = 'event'
			       and "column" = 'match_id'
			       and fk.raw_value is not null				   
				   and e.match_id is null
				  ;	
		 		  

		 		  delete 
		 		    from stg.orphan_foreign_key fk
			       using event e, 
			       		 match m
				 where e.match_id = m.match_id 
				   and fk.raw_value = m.opta_match_uid 
				   and fk.pk_value = e.opta_event_uid
			       and "table" = 'event'
			       and "column" = 'match_id'
					; 		
				
	
	 		update event e
				   set event_player_id = m.player_id , update_tms = current_timestamp, update_audit_id = v_audit_id
				  from stg.orphan_foreign_key fk, 
				  		"player" m
				 where fk.raw_value = m.opta_player_uid
				   and e.opta_event_uid = fk.pk_value
			       and "table" = 'event'
			       and "column" = 'player_id'
			       and fk.raw_value is not null				   
				   and e.event_player_id is null
				  ;					
			
				 
		 		delete 
		 		  from stg.orphan_foreign_key fk
			     using event e, 
			       		 player m
				 where e.event_player_id = m.player_id 
				   and fk.raw_value = m.opta_player_uid 
				   and fk.pk_value = e.opta_event_uid
			       and "table" = 'event'
			       and "column" = 'player_id'
					; 					
				

	 		update event e
				   set event_team_id = m.team_id , update_tms = current_timestamp, update_audit_id = v_audit_id
				  from stg.orphan_foreign_key fk, 
				  		team m
				 where fk.raw_value = m.opta_team_uid 
				   and e.opta_event_uid = fk.pk_value
			       and "table" = 'event'
			       and "column" = 'team_id'
			       and fk.raw_value is not null				   
				   and e.event_team_id is null
				  ;						
				
	
		 		delete 
		 		  from stg.orphan_foreign_key fk
			     using event e, 
			       		 team m
				 where e.event_team_id = m.team_id 
				   and fk.raw_value = m.opta_team_uid 
				   and fk.pk_value = e.opta_event_uid
			       and "table" = 'event'
			       and "column" = 'team_id';				
				
				 update audit
					set status = 'success', end_tms = clock_timestamp()::timestamp
				  where audit_id = v_audit_id;
					 
				 p_audit_id := v_audit_id;
				
	 	END;
 	$$;		
 