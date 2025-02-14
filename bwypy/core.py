try:
    import ujson as jsonlib
except:
    import json as jsonlib
import pandas as pd
import logging
import time
import urllib.request
import numpy as np
import copy
import os

from collections import defaultdict
_globals = defaultdict(lambda: None)

class set_options(object):

    def __init__(self, **kwargs):
        self.old = _globals.copy()
        _globals.update(kwargs)

    def __enter__(self):
        return

    def __exit__(self, type, value, traceback):
        _globals.clear()
        _globals.update(self.old)

class BWQuery:
    default = {"search_limits": {},
               "words_collation": "Case_Sensitive",
               "method": "data",
               "format":"json",
               "counttype": ["TextCount", "WordCount"], "groups": []}

    def __init__(self, json=None, endpoint=None, database=None, verify_fields=True, verify_cert=True):
        '''
        verify_fields: Whether to ask the server for the allowable fields and
            verify later calls accordingly. Turn this offer for a performance
            improvement, because it saves a call to the server.
            
            Validation checks are always done if fields are available. If you turn
            off verify_fields but later run `fields()`, checks will resume.
        '''
        self._fields = None
        self._last_good = None
        # Explicit data type definition
        self._dtypes = {}
        self._field_cache = {}
        # Allow turning off SSL verification if there are cert issues
        self._verify_cert = verify_cert
        
        if json:
            if type(json) == dict:
                self.json = json
            else:
                self.json = jsonlib.decode(json)
        else:
            self.json = copy.deepcopy(self.default)
            
        if endpoint:
            self.endpoint = endpoint            
        elif 'endpoint' in _globals:
            self.endpoint = _globals['endpoint']
        else:
            raise NameError("No endpoint. Provide to BWQuery on initialization "
                            "or set globally.")
        
        if database:
            self.json['database'] = database
        elif ('database' not in self.json) and ('database' in _globals):
            self.json['database'] = _globals['database']
        if not self.json['database']:
            raise NameError("No database specified. Provide to BWQuery "
                            "on initialization as an arg or as part of "
                            "the query, or set it globally.")
        
        # Run check for all available fields
        if verify_fields:
            self.fields()
        
        self._validate()

    def _validate(self):
        '''
        Check for proper formatting
        '''
        try:
            if self.json['method'] not in ["data"]:
                logging.warn('Ignoring custom method argument. Results are parsable in various formats')
                self.json['method'] = "data"

            for prop in ['groups', 'search_limits']:
                validate_func = getattr(self, '_validate_' + prop)(getattr(self, prop))

            # Because of the way some setters work, it's worthwhile keeping the last known 'good' copy
            self._last_good = copy.deepcopy(self.json)
        except:
            if self._last_good is not None:
                self.json = copy.deepcopy(self._last_good)
            raise
            
    
    def _runtime_validate(self):
        ''' 
        Query issues that we can tolerate until somebody tries to run the thing
        '''
        pass
        #if len(self.groups) == 0:
        #    raise ValueError("Need at least one grouping field. Try setting with `groups` method.")
    
    @property
    def groups(self):
        return self.json['groups']
    
    @groups.setter
    def groups(self, value):
        self._validate_groups(value)
        self.json['groups'] = value
            
    def _validate_groups(self, value):
        if self._fields is not None:
            accepted = (self._fields['name'].tolist() +
                    (self._fields['name'] + '__id').tolist() +
                    ('*' + self._fields['name']).tolist() +
                    ('*' + self._fields['name'] + '__id').tolist()
                    )
            badgroups = np.setdiff1d(value, accepted)
            if len(badgroups) > 0:
                raise KeyError("The following groups are not supported in this BW: %s" % ", ".join(badgroups))
        
    @property
    def database(self):
        return self.json['database']
    
    @database.setter
    def database(self, value):
        self.json['database'] = value
        
    @property
    def search_limits(self):
        if 'search_limits' in self.json:
            return self.json['search_limits']
        else:
            return {}
    
    @search_limits.setter
    def search_limits(self, value):
        self._validate_search_limits(value)
        self.json['search_limits'] = value
        
    def _validate_search_limits(self, value):
        if self._fields is not None:
            accepted = (self._fields['name'].tolist() +
                    (self._fields['name'] + '__id').tolist() +
                    ['word']
                    )
            badgroups = np.setdiff1d(list(value.keys()), accepted)
            if len(badgroups) > 0:
                raise KeyError("The following search_limit fields are not supported in this BW: %s" % ", ".join(badgroups))
                
        if ('word' in value) and type(value['word']) is not list:
            raise TypeError("word value needs to be a list, even if there is only one word.")
            
        
    @property
    def counttype(self):
        return self.json['counttype']
    
    @counttype.setter
    def counttype(self, value):
        self.json['counttype'] = value

    def fields(self):
        '''
        Return Pandas object with all the fields in a Bookworm
        '''
        if self._fields is None:
            q = {'database': self.json['database'],
                 'method': 'returnPossibleFields'}#,
#                 'format': 'json',
#                 'counttype': self.counttype,
#                 'groups': self.groups}
            obj = self._fetch(q)
            df = pd.DataFrame(obj)
            self._fields = df
            self._dtypes = df[['name', 'type']].set_index('name').to_dict()['type']
        return self._fields
                     
    def run(self):
        self._validate()
        self._runtime_validate()
            
        logging.debug("Running " + jsonlib.dumps(self.json))
        json_response = self._fetch(self.json)
        
        return BWResults(json_response['data'], self.json, self._dtypes)

    def field_values(self, field, max=None):
        ''' Return all possible values for a field. '''
        if field not in self._field_cache:
            q = copy.deepcopy(self.default)
            q['database'] = self.database
            if max is not None:
                q['search_limits'] = { field+'__id': { '$lt' : max+1} }
                q['groups'] = '*'+field
            else:
                q['groups'] = field
            json_response = self._fetch(q)
            values = (BWResults(json_response, q).dataframe()
                            .sort_values('TextCount', ascending=False)
                            .index
                            .tolist())
            self._field_cache[field] = values
        return self._field_cache[field]
    
    def limited_field_values(self, field):
        q = copy.deepcopy(self.json)
        try:
            del q['search_limits']['word']
        except:
            pass
        if type(q['groups']) is list:
            q['groups'].append(field)
        else:
            q['groups'] = [q['groups'], field]
            
        json_response = self._fetch(q)
        values = (BWResults(json_response, q).dataframe()
                  .sort_values('TextCount', ascending=False)
                  .index
                  .tolist())
        self._field_cache[field] = values
        
    def stats(self):
        q = self.default.copy()
        # Let's hope nobody creates a bookworm on the history of the universe:
        q['search_limits'] = [{"date_year": {"$lte": 10000}}]
        return self.search(q)

    def _fetch(self, query):
        ''' Get results from a bookworm server
            This method calls JSON and converts to Pandas, rather than using
            Bookworm's built-in DataFrame return method, as JSON is a more
            transparent and safer format for data interchange.
        '''
        start = time.time()
        qurl = "%s?queryTerms=%s" % (self.endpoint, jsonlib.dumps(query))
        try:
            f = urllib.request.urlopen(qurl)
            response = jsonlob.loads(f.read())
        except:
            # Python 3, being lazy here
            import requests
            r = requests.get(qurl, verify=self._verify_cert)
            response = r.json()
        logging.debug("Query time: %ds" % (time.time()-start))
        return response
        

class BWResults:

    def __init__(self, results, query, dtypes={}):
        self._json = results
        if type(query['groups']) is list:
            self.groups = query['groups']
        else:
            self.groups = [query['groups']]
            
        if type(query['counttype']) is list:
            self.counttype = query['counttype']
        else:
            self.counttype = [query['counttype']]
        self.dtypes = dtypes
        
        # Results don't care about leading '*'
        self.groups = [g.lstrip("*") for g in self.groups]
    
    def frame(self, index=True, drop_zeros=False, drop_unknowns=False):
        df = pd.DataFrame(self.tolist())
        
        for k,v in self.dtypes.items():
            if k in df:
                if v == 'integer':
                    df[k] = pd.to_numeric(df[k])
                elif v == 'datetime':
                    df[k] = pd.to_datetime(df[k])
                #elif v == 'character':
                #    df[k] = df[k].str.encode('utf8','replace')
                    
        # Drop unknown values
        if drop_unknowns:
            blacklist = ["No place, unknown, or undetermined", "", " ", "Unknown", "unknown",
             "Unknown or not specified", "No attempt to code", "Undetermined", "|||",
             "???", "N/A", "und", "unk"]
            df = df[~df.T.isin(blacklist).any()]
        
        # Set index
        if len(self.groups) > 0 and index:
            df2 = df.set_index(self.groups)
        else:
            df2 = df[self.groups + self.counttype]
        
        # Drop rows with zero for any count type
        if drop_zeros:
            df3 = df2[(df2.T != 0).any()].sort_values(self.counttype, ascending=False)
        else:
            df3 = df2.sort_values(self.counttype, ascending=False)
        
        return df3
        
    def dataframe(self, **args):
        ''' Alias for frame '''
        return self.frame(**args)
    
    def json(self):
        return self._json
    
    def csv(self, **args):
        '''
        This wraps Pandas DataFrame.to_csv, so all valid arguments there
        are accepted here.
        
        https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_csv.html
        '''
        return self.dataframe(index=True).to_csv(**args)
    
    def tuples(self):
        ''' Return a list of tuples '''
        return [tuple(row) for row in self.dataframe(index=False).values]
    
    def tolist(self):
        ''' Return a list of key value pairs for each count'''
#        logging.debug(self._json)
        if 'data' in self._json:
            return self._expand(self._json['data'], self.groups, self.counttype)
        else:
            return self._expand(self._json, self.groups, self.counttype)
    
    def _expand(self, o, grouplist, counttypes, collector=[]):
        '''
        A recursive method for exploding results into rows, one line per set of
        facets
        '''
        new_coll = []
#        logging.debug(o)
#        logging.debug(type(o))
        if len(grouplist) == 0:
            l = []
            for i, val in enumerate(o):
                counttype = counttypes[i]
                l += [(counttype, val)]
            return [dict(collector + l)]
        else:
            l = []
            for k, v in o.items():
                item = (grouplist[0], k)
                new_coll = collector + [item]
                l += self._expand(v, grouplist[1:], counttypes, new_coll)
            return l
