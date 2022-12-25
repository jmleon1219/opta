drop procedure upsert_qualifier; 	
create or replace procedure upsert_qualifier(in p_audit_id audit.audit_id%TYPE, in p_audit_tms audit.start_tms%type, in p_file_name audit.file_name%type, in p_file_tms audit.file_tms%type)
	language sql
	as $$
	
		with nr as (
			 	select distinct
			 		   ec.code_id,
			 		   fps.value,
			 		   fps.q_uid,
			 		   fps.qualifier_id,
			 		   e.event_id
			 	  from stg.f24_parsed_stg fps
			 left join code ec
					on cast(fps.qualifier_id as int) = ec.source_id 
			   	   and ec."type" = 'qualifier_type'
			 left join event e
			 		on fps.e_uid = e.opta_event_uid
			 	 where fps.file_name = p_file_name
			 	   and fps.file_timestamp = p_file_tms
			 	   and fps.q_uid is not null 		
		)	
	  insert into qualifier(qualifier_code_id, value, opta_qualifier_uid, opta_qualifier_id,event_id, insert_tms, insert_audit_id)
			select code_id, value, q_uid, qualifier_id, event_id, p_audit_tms, p_audit_id
			  from nr
	$$;
