from lxml import etree
from io import StringIO
from sqlalchemy import engine
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql import text
from sqlalchemy.types import String, DateTime, Integer
import boto3
import pandas as pd
import sys
import logging
import os
from json import loads

conn = None

def lambda_handler(event, context):
	try:
		global conn
		fn = None
		val_audit = None
		logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s", level=logging.INFO, force=True)
		logging.info(event)
		if conn == None:
			logging.info("Getting Username and Password from secrets manager")
			sm_client = boto3.client('secretsmanager')
			secret = loads(sm_client.get_secret_value(SecretId=tracking)["SecretString"])
			proxy = loads(sm_client.get_secret_value(SecretId=postgresql-proxy)["SecretString"])
			logging.info("Creating connection to Postgres")
			eng = engine.create_engine(f"postgresql+psycopg2://{proxy['username']}:{proxy['password']}@{proxy['host']}/tracking", executemany_mode='values', executemany_values_page_size=2500)
			conn = eng.connect().execution_options(isolation_level="AUTOCOMMIT")				

		for record in event['Records']:
			bucket, key = record['messageAttributes']['s3_path']['stringValue'].split('/')
			fn = record['messageAttributes']['file_name']['stringValue']
			logging.info(f"Processing file: {fn}")
			val_audit = None
	
			client = boto3.resource("s3")
			f =client.Object(bucket, "{}/{}".format(key,fn))
			try:
				body = f.get()['Body'].read()
			except client.meta.client.exceptions.NoSuchKey:
				logging.info("Key has already been processed")
				continue
				
			f.delete()
	
			tree = etree.fromstring(body)
			games = tree.xpath("//Game")
	
			event_qual_l = []
	
			for g in games:
				events = g.xpath("./Event")
				game_d = {"file_timestamp" : tree.get("timestamp"), "file_name" : fn, "season_name": g.get("season_name"), "season_id": g.get("season_id"), "period_1_start": g.get("period_1_start"), "period_2_start": g.get("period_2_start"), "matchday": g.get("matchday"), "home_team_name": g.get("home_team_name"), "home_team_id": g.get("home_team_id"), "home_score": g.get("home_score"), "game_date": g.get("game_date"), "competition_name": g.get("competition_name"), "competition_id": g.get("competition_id"), "away_team_name": g.get("away_team_name"), "away_team_id": g.get("away_team_id"), "away_score": g.get("away_score"),"g_uid": g.get("id")}
				for e in events:
					evt_d = {}
					evt_d = {"timestamp":e.get("timestamp"), "e_uid":e.get("id"), "version":e.get("version"), "last_modified":e.get("last_modified"), "y": e.get("y"), "x": e.get("x"), "outcome": e.get("outcome"), "team_id":e.get("team_id"), "sec": e.get("sec"), "min": e.get("min"), "period_id":e.get("period_id"), "type_id":e.get("type_id"), "event_id":e.get("event_id") , "player_id":e.get("player_id")}
					evt_d.update(game_d)
					qual = e.xpath("./Q")
					q_l=[]
					for q in qual:
						qual_d = {}
						qual_d.update({"q_uid":q.get("id"), "value":q.get("value"), "qualifier_id":q.get("qualifier_id")})
						qual_d.update(evt_d)
						q_l.extend([qual_d])
					if len(q_l) == 0:
						q_l = [evt_d]
					event_qual_l.extend(q_l)
	
			game_df = pd.DataFrame(event_qual_l)
			game_df.loc[game_df.last_modified == '0000-00-00T00:00:00', 'last_modified'] = pd.NA
			logging.info(f"Processed {len(game_df.index)} records")
			
			try:
				game_df.to_sql(name='f24_parsed_stg', con=conn, schema='stg', index=False, if_exists="append")
				
				res = conn.execute(text('call process_f24(:fn , cast(:ftms as timestamp), :audit_id )') \
										.bindparams(bindparam("fn", type_=String), bindparam("ftms", type_=String), bindparam("audit_id", type_= Integer, isoutparam=True)) \
									, {"fn": fn, "ftms":tree.get("timestamp"), "audit_id": -1})
									
				res_audit = res.fetchone()
				val_audit = res_audit.items()[0][1]
				logging.info(f"Corresponding audit_id is {val_audit}")
			except Exception as e:
				logging.info("Writing Error to Audit Table")
				del_f24_stg = text("""delete from stg.f24_parsed_stg where file_name = :p_file_name and file_timestamp = :p_file_tms""")
				conn.execute(del_f24_stg, {"p_file_name": fn, "p_file_tms": tree.get("timestamp")}) if conn != None else None
				
				upd_audit_sql = text("""update audit set status = 'fail', end_tms = clock_timestamp(), message = :error_message where audit_id = :v_audit_id""")
				conn.execute(upd_audit_sql, {"v_audit_id": val_audit, "error_message": str(e)}) if conn != None else None
				raise
			finally:
				pass

			logging.info("Data Upload Complete!")
	except Exception as ex:
		sns = boto3.client('sns', region_name='us-east-2')
		subject = "Critical Error - Opta Ingest Real Time - {} Failed".format(fn)
		sns.publish(TargetArn=realtime-sporting-alerts,
					Message = str(ex),
					Subject = subject
					)
		raise
