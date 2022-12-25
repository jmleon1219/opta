from lxml import etree
from io import StringIO
from sqlalchemy import engine
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql import text
from sqlalchemy.types import String, DateTime, Integer
import boto3
import pandas as pd
import logging, traceback
from json import loads

def lambda_handler(event, context):
	fn = None
	conn = None
	try:
		logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s", level=logging.INFO, force=True)
		for record in event['Records']:
			bucket = record['s3']['bucket']['name']
			key = record['s3']['object']['key']
			fn = key.split('/')[-1]
			logging.info(f"Processing file: {fn}")
	
			client = boto3.resource("s3")
			f = client.Object(bucket, key)
			try:
				body = f.get()['Body'].read()
			except client.meta.client.exceptions.NoSuchKey:
				logging.info("Key has already been processed")
				return
			f.delete()
			tree = etree.fromstring(body)
	
			match_officials_l = []
			match_info_l = []
			team_goal_l = []
			timing_l = []
			teams_l = []
			for t in tree:
				sd_d = dict(t.items())
				for m in t.xpath("./MatchData"):
					mi_d = {}
					md_d = dict(m.items())
					md_d.update({"file_name":fn})
					for s in m.xpath("./Stat"):
						md_d.update({s.get("Type"): s.text}) if s.get("Type") == 'Venue' else None
						md_d.update({s.get("Type"): s.text}) if s.get("Type") == 'City' else None
					for td in m.xpath("./TeamData"):
						md_d.update({td.get("Side").lower():td.get("TeamRef")}) if td.get("Side").lower() == 'home' or td.get("Side").lower() == 'away' else None
					for mi in m.xpath("./MatchInfo"):
						mi_d = dict(mi.items())
						mi_d.update({"match_date":mi.xpath("./Date")[0].text})
						mi_d.update({"match_tz":mi.xpath("./TZ")[0].text})
						mi_d.update(md_d)
						mi_d.update(sd_d)
						match_info_l.extend([mi_d])
					for mos in m.xpath("./MatchOfficials"):
						mos_d = dict(mos.items())
						for mo in mos.xpath("./MatchOfficial"):
							mo_d = dict(mo.items())
							mo_d.update(mos_d)
							mo_d.update(md_d)
							mo_d.update(sd_d)
							match_officials_l.extend([mo_d])
					for tdg in m.xpath("./TeamData/Goal"):
						team_goal_d = dict(tdg.getparent().items())
						team_goal_d.update(tdg.items())
						team_goal_d.update({"g_id":m.get("uID")})
						team_goal_l.extend([team_goal_d])
				for dt in t.xpath("./TimingTypes/DetailTypes/DetailType"):
					timing_det_d = {"type":"detail_id", "id":dt.get("detail_id"), "name":dt.get("name")}
					timing_l.extend([timing_det_d])
				for tat in t.xpath("./TimingTypes/TimestampAccuracyTypes/TimestampAccuracyType"):
					timing_acc_d = {"type":"timestamp_accuracy_id", "id":tat.get("timestamp_accuracy_id"), "name":tat.get("name")}
					timing_l.extend([timing_acc_d])
				for tt in t.xpath("./TimingTypes/TimingType/TimingType"):
					timing_typ_d = {"type":"timing_id", "id":tt.get("timing_id"), "name":tt.get("name")}
					timing_l.extend([timing_typ_d])
				for tm in t.xpath("./Team/Name"):
					teams_d = dict(tm.getparent().items())
					teams_d.update({tm.tag : tm.text, "file_name": fn})
					teams_l.extend([teams_d])
	
	
			officials_df =pd.DataFrame(match_officials_l).rename(columns={"FirstName": "first_name", "LastName":"last_name", "Type":"type", "uID":"o_id"})
			match_info_df =pd.DataFrame(match_info_l).rename(columns={"MatchDay":"match_day", "MatchType":"match_type", "MatchWinner":"match_winner", "Period":"period", "Venue_id":"venue_id", "uID":"g_id", "Type":"type", "Venue":"venue", "City":"city", "AdditionalInfo":"additional_info", "Leg":"leg", "RoundType":"round_type", "FirstLegId":"first_leg_id", "timestamp":"file_timestamp"})
			teams_df =pd.DataFrame(teams_l).rename(columns={"uID":"t_id", "Name":"team_name"})
			timing_df = pd.DataFrame(timing_l)
			team_goal_df =pd.DataFrame(team_goal_l).rename(columns={"HalfScore":"half_score", "Score":"score", "Side":"side", "TeamRef":"t_id", "Period":"period", "PlayerRef":"p_id", "Type":"type"})
	
			match_full_df = match_info_df.merge(timing_df.loc[timing_df['type']=='detail_id'],left_on='detail_id',right_on='id', how="left") \
			.merge(timing_df.loc[timing_df['type']=='timestamp_accuracy_id'],left_on='timestamp_accuracy_id',right_on='id', how="left") \
			.merge(timing_df.loc[timing_df['type']=='timing_id'],left_on='timing_id',right_on='id', how="left")
	
			match_full_df['file_timestamp']= pd.to_datetime(match_full_df["file_timestamp"], format='%Y-%m-%d %H-%M-%S')
			match_full_df.loc[match_full_df['timestamp_accuracy_id'] == '', 'timestamp_accuracy_id'] = pd.NA
			match_full_df.loc[match_full_df['timing_id'] == '', 'timing_id'] = pd.NA
			match_full_df.loc[match_full_df['detail_id'] == '', 'detail_id'] = pd.NA
	
			match_full_df = match_full_df.drop(labels=['type_y','type_x','id_x', 'id_y' ,'id'], axis=1).rename(columns={'name':'timing_type','name_x':'timing_detail_type','name_y':'timestamp_accuracy_type'})
	
			cols = ["match_day","match_type","match_winner","period","venue_id","home","away","match_date","match_tz","detail_id","last_modified","timestamp_accuracy_id","timing_id","g_id","venue","city","competition_code","competition_id","competition_name","game_system_id","season_id","season_name","file_timestamp","additional_info","leg","round_type","first_leg_id","timing_detail_type","timestamp_accuracy_type","timing_type","file_name"]
			col_diff = set(cols) - set(match_full_df.columns.tolist())
			for c in col_diff:
				match_full_df[c] = pd.NA
	
			match_full_df = match_full_df[cols]
	
			logging.info(f"Processed {len(match_full_df.index)} match records")
			logging.info(f"Processed {len(teams_df.index)} teams records")
			logging.info(f"Processed {len(team_goal_df.index)} team goal records")
	
			logging.info("Getting Username and Password from secrets manager")
			sm_client = boto3.client('secretsmanager')
			secret = loads(sm_client.get_secret_value(SecretId=postgresql-proxy)["SecretString"])
	
			logging.info("Creating connection to Postgres")
			
			try:
				f_tms = str(match_full_df['file_timestamp'][0])
				eng = engine.create_engine(f"postgresql+psycopg2://{secret['username']}:{secret['password']}@{secret['host']}/tracking", executemany_mode='values', executemany_values_page_size=2500)
				conn = eng.connect().execution_options(isolation_level="AUTOCOMMIT")
		
				match_full_df.to_sql(name='match_stg', con=conn, schema='stg', index=False, if_exists="append")
				teams_df.to_sql(name='team_stg', con=conn, schema='stg', index=False, if_exists="append")
		
				
				res = conn.execute(text('call process_results(:fn , cast(:ftms as timestamp), :audit_id )') \
								.bindparams(bindparam("fn", type_=String), bindparam("ftms", type_=String), bindparam("audit_id", type_= Integer, isoutparam=True)) \
							, {"fn": fn, "ftms":f_tms, "audit_id": -1})
							
				res_audit = res.fetchone()
				val_audit = res_audit.items()[0][1]	
				logging.info(f"Corresponding audit_id is {val_audit}")
			except Exception as e:
				logging.info("Writing Error to Audit Table")
				del_match_stg = text("""delete from stg.match_stg where file_name = :p_file_name and file_timestamp = :p_file_tms""")
				del_team_stg = text("""delete from stg.team_stg where file_name = :p_file_name""")
				if conn:
					conn.execute(del_match_stg, {"p_file_name": fn, "p_file_tms": f_tms})
					conn.execute(del_team_stg, {"p_file_name": fn})
				
					upd_audit_sql = text("""update audit set status = 'fail', end_tms = clock_timestamp(), message = :error_message where audit_id = :v_audit_id""")
					conn.execute(upd_audit_sql, {"v_audit_id": val_audit, "error_message": str(e)})
				raise
			finally:
				conn.close() if conn else None
			logging.info("Data Upload Complete!")
	except Exception as ex:
		sns = boto3.client('sns', region_name='us-east-2')
		subject = "Critical Error - Opta Ingest Real Time - {} Failed".format(fn)
		sns.publish(TargetArn=realtime-sporting-alerts,
					Message = "{}\n\n{}".format(str(ex), traceback.format_exc()),
					Subject = subject
					)
		raise
