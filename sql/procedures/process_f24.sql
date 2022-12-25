drop procedure process_f24;
create or replace procedure process_f24(in p_file_name audit.file_name%type, p_file_tms audit.file_tms%type, inout p_audit_id int)
			language plpgsql
			security invoker
			as $$ 
	
				declare 
				
					v_audit_id		audit.audit_id%type;
					v_process_tms	timestamp;
					v_err_text 		text;
					v_err_detail    text;
					v_error_message text;

				begin
					v_process_tms := current_timestamp::timestamp;

					insert 
					  into audit(start_tms, status, file_name, file_tms)
					values (v_process_tms, 'running', p_file_name, p_file_tms) 
				 returning audit_id
					  into v_audit_id;

					p_audit_id := v_audit_id; 
					 
					 commit;
					 
					call upsert_event(v_audit_id, v_process_tms, p_file_name, p_file_tms); 
					call upsert_qualifier(v_audit_id, v_process_tms, p_file_name, p_file_tms); 

					delete
					  from stg.f24_parsed_stg
					 where file_name = p_file_name
					   and file_timestamp = p_file_tms;
				
					update audit set status = 'success', end_tms = clock_timestamp() where audit_id = v_audit_id;
					 
				end;
			$$;
