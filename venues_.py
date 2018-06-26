import json
import time
import arrow
import pandas as pd
import sqlalchemy
from sqlalchemy.orm.session import sessionmaker

import pickle

class VenueTableGetter:
	
	"""
	class to connect to venue tables
	"""
	def __init__(self, **kwargs):

		self.VENUE_BASE_TBL = 'DWSales.dbo.venue_dim'
		self.VENUE_EXTRA_TBL = 'DWSales.dbo.VenuesPowerWebAddresses'

		self.VENUES = pd.DataFrame()


	def start_session(self, sqlcredsfile):

		print('starting sqlalchemy session...', end='')

		sql_creds = json.load(open(sqlcredsfile))

		sql_keys_required = set('user user_pwd server port db_name'.split())

		if sql_keys_required != set(sql_creds):
			raise KeyError(f'SQL Credentials are incomplete! The following keys are missing: '
				f'{", ".join([k for k in sql_keys_required - set(sql_creds)])}')

		# generate a Session object
		self._SESSION = sessionmaker(autocommit=True)
		self._ENGINE = sqlalchemy.create_engine(f'mssql+pymssql://{sql_creds["user"]}:{sql_creds["user_pwd"]}@{sql_creds["server"]}:{sql_creds["port"]}/{sql_creds["db_name"]}')
		self._SESSION.configure(bind=self._ENGINE)
		# create a session
		self.sess = self._SESSION()

		print('ok')
		
		return self

	def close_session(self):

		self.sess.close()

		print('closed sqlalchemy session...')

		return self


	def exists(self, tab):
		"""
		check if a table tab exists; return 1 if it does or 0 otherwise
		"""
		return self.sess.execute(f""" IF OBJECT_ID(N'{tab}', N'U') IS NOT NULL
											SELECT 1
										ELSE
											SELECT 0
										  """).fetchone()[0]

	def count_rows(self, tab):
		"""
		count how many rows in table tab
		"""
		return self.sess.execute(f'SELECT COUNT (*) FROM {tab};').fetchone()[0]
				

	def get_venues(self):

		for tbl in [self.VENUE_BASE_TBL, self.VENUE_EXTRA_TBL]:
			if not self.exists(tbl):
				raise Exception(f'table {tbl} doesn\'t exist!')
			else:
				print(f'table {tbl} exists and has {self.count_rows(tbl)} rows...')

		

		self.VENUES = pd.read_sql(f"""
								SELECT venues.*,
									   ven_details.vcName, ven_details.paAddressLine1, ven_details.paAddressLine2, 
									   ven_details.vcRegionName
								FROM
									(SELECT pk_venue_dim, venue_name, venue_desc, venue_region_desc
									 FROM {self.VENUE_BASE_TBL} WHERE venue_name like '[a-Z][a-Z][a-Z]') venues
									 LEFT JOIN
									(SELECT venue_name, vcName, paAddressLine1, paAddressLine2, vcRegionName
									 FROM {self.VENUE_EXTRA_TBL} WHERE venue_name like '[a-Z][a-Z][a-Z]') ven_details
									ON
									venues.venue_name = ven_details.venue_name;
											
								""", self._ENGINE)

		print(f'collected venue information:')
		print(f'{len(self.VENUES)} rows')
		print(f'there are {len(self.VENUES.columns)} columns: {", ".join([c for c in self.VENUES.columns])}')

		return self

	def save(self):

		file_ = f'venues_{arrow.utcnow().to("Australia/Sydney").format("YYYYMMDD")}.csv'
		self.VENUES.to_csv(file_, sep='\t', index=False)

		print(f'saved to file {file_}')
	

if __name__ == '__main__':

	vtg = VenueTableGetter()

	vtg.start_session('config/rds.txt').get_venues()

	vtg.close_session()

	vtg.save()

	print('unpickling model..')
	model = pickle.load(open('badvenue.pkl', 'rb'))

	for t in zip(vtg.VENUES['venue_desc'], model.predict(vtg.VENUES['venue_desc'].tolist())):
		print(f'{t[0]} is {t[1]}')


