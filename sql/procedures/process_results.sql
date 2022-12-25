drop procedure process_results;
create or replace procedure process_results(in p_file_name audit.file_name%type,in p_file_tms audit.file_tms%type, inout p_audit_id int)
			language plpgsql
			as $$ 
	
				declare 
				
					v_audit_id		audit.audit_id%type;
					v_process_tms	timestamp;
				
				begin
					
					v_process_tms := current_timestamp;
						
						insert 
						  into audit(start_tms, status, file_name, file_tms)
						values (v_process_tms, 'running', p_file_name, p_file_tms) 
					 returning audit_id
						  into v_audit_id;
						 
					commit;
						
					call upsert_country(v_audit_id, v_process_tms, p_file_name);	
					call upsert_competition(v_audit_id, v_process_tms, p_file_name); 
					call upsert_team(v_audit_id, v_process_tms, p_file_name);
					call upsert_match(v_audit_id, v_process_tms, p_file_name); 

				
					delete
					  from stg.match_stg
					  where file_name = p_file_name
					    and file_timestamp = p_file_tms
					   ;

					delete
					  from stg.team_stg
					 where file_name = p_file_name
					   ;					 
					 
					 update audit
						set status = 'success', end_tms = clock_timestamp()
					  where audit_id = v_audit_id;

					p_audit_id := v_audit_id;					 				 
				end;
			$$;			
