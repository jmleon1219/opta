from lxml import etree
from io import StringIO
from sqlalchemy import engine
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql import text
from sqlalchemy.types import String, DateTime, Integer
import boto3
import pandas as pd
import logging
from json import loads

def lambda_handler(event, context):
	fn = None
	val_audit = None
	try:
		logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s", level=logging.INFO, force=True)
		for record in event['Records']:
			bucket = record['s3']['bucket']['name']
			key = record['s3']['object']['key']
			fn = key.split('/')[-1]
			logging.info(f"Processing file: {fn}")
	
			client = boto3.resource("s3")
			f =client.Object(bucket, key)
			try:
				body = f.get()['Body'].read()
			except client.meta.client.exceptions.NoSuchKey:
				logging.info("Key has already been processed")
				return
			f.delete()
			tree = etree.fromstring(body)
	
			file_timestamp = tree.attrib['timestamp']
			tree.xpath("./SoccerDocument")
			teams = tree.xpath("./SoccerDocument//Team")
			players_l = []
			exclude_teams = ['winner', 'finalist']
	
			for sd in tree.xpath("./SoccerDocument"):
				sd_d = sd.attrib
				sd_d.update({"file_timestamp" : file_timestamp, "file_name":fn})
				for t in teams:
					team_d = dict()
					if not any([x in t.get("Name") for x in exclude_teams if t.get("Name") != None]):
						team_d = t.attrib
						for e in t.getchildren():
							if len(e.getchildren()) == 0:
								team_d.update({e.tag: e.text}) if e.text != None else None
					for p in t.xpath("./Player"):
						stat = p.xpath("./Stat")
						player_d = {}
						for s in stat:
							s_d = {f"player_{s.get('Type')}" : s.text} if s.get('Type').lower() == 'country' else {s.get('Type') : s.text}
							player_d.update(s_d)
						player_d.update({"p_id":p.attrib['uID']})
						player_d.update({"loan_ind": p.get("loan")}) if p.get("loan") != None else None
						player_d.update(team_d)
						player_d.update(sd_d)
						players_l.extend([player_d])
	
			team_cols = ['official_club_name','short_club_name','team_name','symid','country', 'region_name', 'team_founded']
			players_stg_df = pd.DataFrame(players_l).rename(columns={"uID":"t_id", "Type":"type", "Founded":"team_founded", "Name":"team_name", "SYMID":"symid"})
			players_df = players_stg_df.drop_duplicates(subset=['p_id'], keep='first').reset_index()
			
			cols =["first_name", "last_name", "birth_date", "birth_place", "first_nationality", "weight", "height", "jersey_num", "real_position", "real_position_side", "join_date", "player_country", "country", "p_id", "country_id", "country_iso", "official_club_name", "region_id", "region_name", "short_club_name", "t_id", "team_founded", "team_name","symid","type", "competition_code","competition_id","competition_name","season_id","season_name","file_timestamp","file_name","preferred_foot","on_loan_from","middle_name","known_name","deceased", "loan_ind"]
			col_diff = set(cols) - set(players_df.columns.tolist())
			for c in col_diff:
				players_df[c] = pd.NA			
			
			players_df[['t_id'] + team_cols] = players_df.set_index('t_id').groupby('t_id')[team_cols].ffill().reset_index('t_id')
			players_df['join_date'] = pd.to_datetime(players_df.join_date, format='%Y-%m-%d', errors='coerce')
			players_df['birth_date'] = pd.to_datetime(players_df["birth_date"], format='%Y-%m-%d', errors='coerce')
			players_df.loc[players_df["weight"] == 'Unknown',['weight']] = pd.NA
			players_df.loc[players_df["height"] == 'Unknown',['height']] = pd.NA
			
			logging.info(f"Processed {len(players_df.index)} player records")
			logging.info("Getting Username and Password from secrets manager")
			sm_client = boto3.client('secretsmanager')
			secret = loads(sm_client.get_secret_value(SecretId=postgresql-proxysssssss)["SecretString"])
	
			logging.info("Creating connection to Postgres")
			try:
				eng = engine.create_engine(f"postgresql+psycopg2://{secret['username']}:{secret['password']}@{secret['host']}/tracking", executemany_mode='values', executemany_values_page_size=2500)
				conn = eng.connect().execution_options(isolation_level="AUTOCOMMIT")
		
				players_df[cols].to_sql(name='player_stg', con=conn, schema='stg', index=False, if_exists="append")
				
				res = conn.execute(text('call process_squads(:fn , cast(:ftms as timestamp), :audit_id )') \
								.bindparams(bindparam("fn", type_=String), bindparam("ftms", type_=String), bindparam("audit_id", type_= Integer, isoutparam=True)) \
							, {"fn": sd_d["file_name"], "ftms":sd_d["file_timestamp"], "audit_id": -1})
							
				res_audit = res.fetchone()
				val_audit = res_audit.items()[0][1]	

				logging.info(f"Corresponding audit_id is {val_audit}")
			except Exception as e:
				logging.info("Writing Error to Audit Table")
				del_player_stg = text("""delete from stg.player_stg where file_name = :p_file_name and file_timestamp = :p_file_tms""")
				conn.execute(del_player_stg, {"p_file_name": sd_d["file_name"], "p_file_tms": sd_d["file_timestamp"]})
				
				upd_audit_sql = text("""update audit set status = 'fail', end_tms = clock_timestamp(), message = :error_message where audit_id = :v_audit_id""")
				conn.execute(upd_audit_sql, {"v_audit_id": val_audit, "error_message": str(e)}) if val_audit != None else None
				raise
			finally:
				conn.close()

			logging.info("Data Upload Complete!")
	except Exception as ex:
		sns = boto3.client('sns', region_name='us-east-2')
		subject = "Critical Error - Opta Ingest Real Time - {} Failed".format(fn)
		sns.publish(TargetArn=realtime-sporting-alerts,
					Message = f"The error is: \n{str(ex)}",
					Subject = subject
					)
		raise
