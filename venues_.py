import json
import time
import arrow
import pandas as pd
import sqlalchemy
from sqlalchemy.orm.session import sessionmaker

# import pandas as pd
# import json
import re
import os
from pprint import pprint
from itertools import chain
from collections import defaultdict
import googlemaps

import pickle


class VenueMatcher:
	
	"""
	class to connect to venue tables and get all useful data
	""" 

	def __init__(self, **kwargs):

		self.VENUE_BASE_TBL = 'DWSales.dbo.venue_dim'
		self.VENUE_EXTRA_TBL = 'DWSales.dbo.VenuesPowerWebAddresses'

		self.STRUCT = {'backlog': {'dir': 'backlog', 'file': 'backlog.json'},
						'new_venues': {'dir': 'new_venues', 'file': 'new_venues.csv.gz'},
						'old_venues': {'dir': 'old_venues', 'file': 'processed_venue_codes.txt'}} 

		self.PREFERRED_STATES = 'nsw vic qld wa act sa tas nt'.split()
	
		self.STATES = {'nsw': 'new south wales', 
						'act': 'australian capital territory', 
						'vic': 'victoria',
						'tas': 'tasmania',
						'wa': 'western australia',
						'nt': 'northern teritory',
						'sa': 'south australia',
						'qld': 'queensland'}
		
		# now another dictionary, full to abbreviated
		self.STATES_ = {v: k for k, v in self.STATES.items()}
		
		self.SUBURBS = json.load(open('data/aus_suburbs_auspost_APR2017.json'))

		self.BAD_TYPES = set('political colloquial_area locality natural_feature'.split())
		
		# sometimes we'd like to pick the search results of a particular type only
		self.ENFORCED_TYPES = {'winery': 'food', 'vineyard': 'food', 'zoo': 'zoo'}

		self.GOOGLE_REQUESTS = 0
		self.MAX_GOOGLE_REQUESTS = 1000
		
		self.gmaps = googlemaps.Client(**json.load(open('credentials/google.json')))	

		self.venues_lst = []

	def check(self):

		for what in self.STRUCT:

			print(f'checking for {what}...')

			if not os.path.exists(self.STRUCT[what]['dir']):
				os.mkdir(self.STRUCT[what]['dir'])

			if not os.path.exists(os.path.join(self.STRUCT[what]['dir'], self.STRUCT[what]['file'])):
				print(f'no {what} found...')
			else:
				print(f'found some {what}...')

		return self


	def start_session(self, sqlcredsfile):

		print('starting sqlalchemy session...', end='')

		sql_creds = json.load(open(sqlcredsfile))

		sql_keys_required = set('user user_pwd server port db_name'.split())

		if sql_keys_required != set(sql_creds):
			raise KeyError(f'SQL Credentials are incomplete! The following keys are missing: '
				f'{", ".join([k for k in sql_keys_required - set(sql_creds)])}')

		# generate a Session object	
		self._ENGINE = sqlalchemy.create_engine(f'mssql+pymssql://{sql_creds["user"]}:{sql_creds["user_pwd"]}@{sql_creds["server"]}:{sql_creds["port"]}/{sql_creds["db_name"]}')
		self._SESSION = sessionmaker(autocommit=True, bind=self._ENGINE)
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
				print(f'table {tbl} has {self.count_rows(tbl):,} rows...')

		

		self.venues_ = pd.read_sql(f"""
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

		print(f'venue information collected: {len(self.venues_):,} rows')
		print(f'columns: {", ".join([c for c in self.venues_.columns])}')

		return self

	def save(self, where=None):

		file_ = os.path.join(self.STRUCT[where]['dir'], self.STRUCT[where]['file'])

		if where == 'new_venues':
			self.venues_.to_csv(file_, sep='\t', index=False, compression='gzip')
			
		elif where == 'backlog':
			json.dump(self.venues_lst, open(file_, 'w'))

		print(f'saved {where} to {file_}')
			

		return self

	def select_new_venues(self):
		"""
		keep only venues with NEW pks, i.e. pks not yet processed
		"""
		
		try: 
			old_pks = {l.strip() for l in open(os.path.join(self.STRUCT['old_venues']['dir'], self.STRUCT['old_venues']['file'])).readlines() if l.strip()}
			self.venues_ = self.venues_[~self.venues_['pk_venue_dim'].astype(str).isin(old_pks)]
		except:
			pass

		print(f'there are {len(self.venues_):,} new venues...')

		return self
	
	def _find_state(self, st):
		"""
		find state names in string st; returns a SET of identified names
		"""
		
		states_found = set()
		
		st_norm = self._normalize(st)
		
		for s in (set(self.STATES) | set(self.STATES_)):
			try:
				states_found.add(re.search(r'\b' + s + r'\b', st_norm).group(0))
			except:
				continue
				
		if states_found: # note that these may be either the full or abbreviated state names
			# return full state names to avoid rare ambiguities like WA (Australia) and WA (the US)
			return {s if s not in self.STATES_ else self.STATES_[s] for s in states_found}
		
		return states_found
	
	def _find_suburb(self, st):
		"""
		find suburb names in string st; returns a set of tuples (suburb, state)
		"""
		st_norm = self._normalize(st)
		
		suburbs_found = set()
		
		words_ = st_norm.split()
		
		for i, w in enumerate(words_):
			
			# take first letter of the word
			l1_ = w[0]
			
			# if any suburb names start from this letter..
			if l1_ in self.SUBURBS:
			
				for r in self.SUBURBS[l1_]:
					
					try:
						suburbs_found.add((re.search(r'\b' + r['name'] + r'\b', ' '.join(words_[i:])).group(0), r['state']))
					except:
						continue
						
		return suburbs_found 
	
	def find_venue_state(self):
		
		"""
		look at the available Ticketek venue description fields and try to figure out what state the venue may
		be in; if this isn't clear, collect candidate states;

		returns a list like 

		[{'pk_venue_dim': 1637, 
			'name': 'convention centre', 
				'code': ['acn'], 
					'state': 'sa'},...

		where 
				'state' is the state we managed to find;

		we could have here something like 'state_': ['wa', 'tas'] where
				'state_' are the candidate states we still keep just in case 
		"""
		
		print('searching for venue states..')

		for i, row in enumerate(self.venues_.iloc[:40].iterrows(),1):
			
			if i%20 == 0:
				print(f'processing venue {i}...')
				
			this_venue = defaultdict()
			
			this_venue['pk_venue_dim'] = row[1]['pk_venue_dim']
			this_venue['name'] = self._normalize(row[1]['venue_desc'])
			this_venue['code'] = [row[1]['venue_name'].lower()]
			
			# search for state according to priority until found in one of the columns,
			# then stop
			
			for c in ['venue_desc', 'vcRegionName','venue_region_desc']:
				
				# note: set below may be empty if no self.STATES found
				candidate_states = self._find_state(self._normalize(row[1][c]))
				
				if len(candidate_states) == 1:
					# a single candidate state
					this_venue['state'] = candidate_states.pop()
					break
				else: 
					# many or no candidate self.STATES; need to find suburb 
					for c in ['venue_desc', 'venue_region_desc']:
						
						# note that sub_state may be an empty set
						suburb_state_tuples = self._find_suburb(self._normalize(row[1][c]))
						
						# suppose a single suburb found
						if len(suburb_state_tuples) == 1:
							
							if len(candidate_states) > 0:
								#  enough if its state is among candidate self.STATES
								if list(suburb_state_tuples)[0][1] in candidate_states:
									this_venue['state'] = list(suburb_state_tuples)[0][1]
							else:
								# if no candidate self.STATES
								this_venue['state'] = list(suburb_state_tuples)[0][1]
								
							break
						
						# what if more than one suburb found?
						elif len(suburb_state_tuples) > 1:
							
							# suppose no candidate self.STATES
							if not candidate_states:
								
								# if different self.SUBURBS in THE SAME state
								_ = {s[1] for s in suburb_state_tuples}
								
								if len(_) == 1:
									this_venue['state'] = _.pop()
									break
									
								else:
									# return the longest (in terms of the number of words in suburb name) tuple (first found)
									longest_sub = max(suburb_state_tuples, key=lambda x: len(x[1].split()))
									# only if the suburb name has AT LEAST TWO words
									if len(longest_sub[0].split()) > 1:
										this_venue['state'] = longest_sub[1]
									else:
										# simply add a list of candidate self.STATES
										this_venue['state_'] = list(_)
									break
							else:
								# if we have multiple candidate self.STATES AND multiple self.SUBURBS
								for ss in suburb_state_tuples:
									# pick the first suburb that has its state among state candidates
									if ss[1] in candidate_states:
										this_venue['state'] = ss[1]
										break
										
			self.venues_lst.append(this_venue)
		
		return self
	
	def merge_codes(self, on='name'):
		"""
		merge Ticketek venues with multiple codes; same name - same code
		"""
		print('merging venue codes...')

		before_ = len(self.venues_lst)

		venues_ = []
		# venue names already processed
		nms = set()
		
		for v in self.venues_lst:
			
			if v[on] not in nms:
				venues_.append(v)
				nms.add(v[on])
			else:
				# this name is already available, must be under another code
				for v_ in venues_:
					if v_[on] == v[on]:
						v_['code'].extend(v['code'])
						v_['code'] = list(set(v_['code']))
						
		self.venues_lst = venues_

		after_ = len(self.venues_lst)

		print(f'now have {after_} distinct venues' + ('' if before_ - after_ == 0 else f' ({after_ - before_})'))
			
		return self 
	
	def _normalize(self, st):
		"""
		normalize a string st
		"""
		if not isinstance(st, str):
			return ''

		st = st.lower()
		# replace separators with white spaces
		st = re.sub(r'[-/_.]', ' ', st)
		# keep only letters, numbers and white spaces
		st = ''.join([l for l in st if str(l).isalnum() or str(l).isspace()])
		st = re.sub(r'\s{2,}', ' ', st)
		
		return st.strip()
	
	def _get_fields(self, res):
		"""
		extract fields from a search response
		"""
		up = {'place_id': res.get('place_id', None),
								  'address': res.get('formatted_address', None),
								  'venue_type': res.get('types', None),
								  'coordinates': res['geometry']['location']}
		return up
		
	def get_place_id(self):
		
		"""
		ask Google maps to find places by name; the key here is to hopefully
		grab a place id
		"""
		# grab whatever is in the backlog if anything..

		try:

			pks = {_['pk_venue_dim'] for _ in self.venues_lst}

			bl_ = [r for r in json.load(open(os.path.join(self.STRUCT['backlog']['dir'], self.STRUCT['backlog']['file']))) 
											if ('place_id' not in r) and (r['pk_venue_dim'] not in pks)]

			print(f'adding {len(bl_)} backlogged venues...')

			self.venues_lst.extend(bl_)

			print(f'now have {len(self.venues_lst)} venues to process...')
			
		except:
			pass

		print('retrieving place ids...')

		for i, v in enumerate(self.venues_lst, 1):
			
			# we want to query Google Maps for the venues that don't have a place_id yet
			
			if 'place_id' not in v:
				
				print(f"{i:04d}: {v['name']}")
					  
				if 'state' in v:
				
					# so we have a specific state..
					try:
						qr_ = self.gmaps.geocode(' '.join([v['name'], self.STATES[v['state']], 'australia']))
						self.GOOGLE_REQUESTS += 1
					except:
						print(f'exceeded quota?')
						self.save('backlog')
						break
				
					if qr_:
						v.update(self._get_fields(qr_[0]))
			
				else:
				
					# problem with the state, need to consider multiple candidates
				
					for possible_state in v['state_']:
						
						try:
							qr_ = self.gmaps.geocode(' '.join([v['name'], self.STATES[possible_state], 'australia']))
							self.GOOGLE_REQUESTS += 1
						except:
							print(f'exceeded quota?')
							self.save('backlog')
							break
					
						if qr_:
							
							q_top_result = None
							
							for r in qr_:

								if self.BAD_TYPES & set(r.get('types',[])):
									continue
								else:
									q_top_result = r
								 
							if q_top_result:
								
								for address_component in q_top_result['address_components']:
									# if the state we search for is in 
									# the result components, we say it's a suitable result
								
									if address_component['short_name'].strip().lower() == possible_state:
										
										v.update(self._get_fields(q_top_result))
										
										break

									# if result has a wrong country
									if ('country' in address_component['long_name']['types']) and (address_component['long_name'].lower() != 'australia'):

										pass
		
		print('completed..')
		self.save('backlog')
		
		return self
	
	def get_place_details(self, local_file=None):
		
		"""
		ask Google maps for place details using a place id; 
		"""
		
		print('retrieving place details...')
		
		if local_file:
			
			self.venues_lst = json.load(open(local_file))
			print(f'collected {len(self.venues_lst)} venues from the locally saved file {local_file}')
			print(f'{sum(["name_googlemaps" in v for v in self.venues_lst])} of these have googlemaps name')
		
		for i, v in enumerate(self.venues_lst, 1):
			
			if i%100 == 0:
				print(f'venue {i}: {v["name"].upper()}...')
			
			if ('place_id' in v) and ('name_googlemaps' not in v):     
				
				try:
					place_details = self.self.gmaps.place(v['place_id'])['result']
					VenueMatcher.self.GOOGLE_REQUESTS += 1
					print(f'requests: {VenueMatcher.self.GOOGLE_REQUESTS}')
				except:
					print(f'can\'t get any place details for place_id {v["name"]}. EXCEEDED QUOTA?')
					json.dump(self.venues_lst, open('data/tkt_venues.json','w'))
					return self                
					  
				try:
					v.update({'name_googlemaps': place_details['name'].lower()})
				except:
					print(f'no googlemap name found!')

				try:
					  v.update({'opening_hours': [d.lower() for d in place_details['opening_hours']['weekday_text']]})
				except:
					  print(f'no opening_hours found!')

				try:     
					 v.update({'rating': float(place_details['rating'])})
				except:
					 print(f'no rating found!')

				try:
					v.update({'url_googlemaps': place_details['url']})
				except:
					print(f'no url found!')

				try:
					v.update({'website': place_details['website']})
				except:
					 print(f'no website found!') 
		
		
		json.dump(self.venues_lst, open('data/tkt_venues.json','w'))
		
		return self
	
	def clear_suspects(self):
		"""
		remove all fields but name, code and state/state candidates for dubious venues
		"""
		
		print('looking for suspicious venues..')
		
		vs_ = [] 
		
		for v in self.venues_lst:
			
			if set(v.get("venue_type", [])) & VenueMatcher.self.BAD_TYPES:
				
				if 'name' not in v:
					continue
				
				fields_ = {'name': v['name'],
						   'code': v['code']}
				if 'state' in v:
					fields_.update({'state': v['state']})
				if 'state_' in v:
					fields_.update({'state_': v['state_']})
					
				vs_.append(fields_)
				
				VenueMatcher.SUSPICIOUS_VENUES.add(v['name'])
				
			else:
				vs_.append(v)
				
		self.venues_lst = vs_
		
		json.dump(self.venues_lst, open('data/tkt_venues.json','w'))
		
		print(f'flagged venues: {len(VenueMatcher.SUSPICIOUS_VENUES)}')
		
		return self
	
	def create_dataset(self):
		
		good_ = set()
		
		for v in self.venues_lst:
			if 'place_id' in v:
				good_.add(v['name'])
		
		pd.DataFrame(pd.concat([pd.DataFrame({'venue': list(good_), 'is_ok': [1]*len(good_)}),
							   pd.DataFrame({'venue': list(VenueMatcher.SUSPICIOUS_VENUES), 'is_ok': [0]*len(VenueMatcher.SUSPICIOUS_VENUES)})])).sample(frac=1.).to_csv('venue_db.csv', index=False)
				

if __name__ == '__main__':

	vm = VenueMatcher() \
		.start_session('config/rds.txt') \
		.check().get_venues() \
		.close_session() \
		.save('new_venues') \
		.select_new_venues() \
		.find_venue_state() \
		.merge_codes() \
		.save('backlog') \
		.get_place_id()


	# print('unpickling model..')

	# model = pickle.load(open('badvenue.pkl', 'rb'))

	# venue_df['is_ok'] = model.predict(venue_df['venue_desc'])

	# save(venue_df)

	# print(venue_df.head())

	# vm = VenueMatcher(venue_df=venue_df)

	# vm.find_venue_state()

	# vm.merge_codes()

	# vm.get_place_id()

	# print(vm.tkt_venues)



