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

        map_to_human_readable = {
            "genres": {
                "http://id.loc.gov/vocabulary/marcgt/bib": "bibliography",
                "http://id.loc.gov/vocabulary/marcgt/gov": "government publication",
                "http://id.loc.gov/vocabulary/marcgt/fic": "fiction",
                "http://id.loc.gov/vocabulary/marcgt/bio": "biography",
                "http://id.loc.gov/vocabulary/marcgt/sta": "statistics",
                "http://id.loc.gov/vocabulary/marcgt/cpb": "conference publication",
                "http://id.loc.gov/vocabulary/marcgt/cat": "catalog",
                "http://id.loc.gov/vocabulary/marcgt/aut": "autobiography",
                "http://id.loc.gov/vocabulary/marcgt/dir": "directory",
                "http://id.loc.gov/vocabulary/marcgt/dic": "dictionary",
                "http://id.loc.gov/vocabulary/marcgt/leg": "legislation",
                "http://id.loc.gov/vocabulary/marcgt/law": "law report or digest",
                "http://id.loc.gov/vocabulary/marcgt/rev": "review",
                "http://id.loc.gov/vocabulary/marcgt/ind": "index",
                "http://id.loc.gov/vocabulary/marcgt/abs": "abstract or summary",
                "http://id.loc.gov/vocabulary/marcgt/han": "handbook",
                "http://id.loc.gov/vocabulary/marcgt/fes": "festschrift",
                "http://id.loc.gov/vocabulary/marcgt/enc": "encyclopedia",
                "http://id.loc.gov/vocabulary/marcgt/yea": "yearbook",
                "http://id.loc.gov/vocabulary/marcgt/ter": "technical report",
                "http://id.loc.gov/vocabulary/marcgt/the": "thesis",
                "http://id.loc.gov/vocabulary/marcgt/lea": "legal article",
                "http://id.loc.gov/vocabulary/marcgt/lec": "legal case and case notes",
                "http://id.loc.gov/vocabulary/marcgt/sur": "survey of literature",
                "http://id.loc.gov/vocabulary/marcmuscomp/sg": "Songs",
                "http://id.loc.gov/vocabulary/marcgt/dis": "discography",
                "http://id.loc.gov/vocabulary/marcmuscomp/co": "Concertos",
                "http://id.loc.gov/vocabulary/marcmuscomp/sn": "Sonatas",
                "http://id.loc.gov/vocabulary/marcgt/fil": "filmography",
                "http://id.loc.gov/vocabulary/marcmuscomp/sy": "Symphonies",
                "http://id.loc.gov/vocabulary/marcmuscomp/ms": "Masses",
                "http://id.loc.gov/vocabulary/marcmuscomp/op": "Operas",
                "http://id.loc.gov/vocabulary/marcmuscomp/zz": "Other",
                "http://id.loc.gov/vocabulary/marcmuscomp/df": "Dance forms",
                "http://id.loc.gov/vocabulary/marcmuscomp/vr": "Variations",
                "http://id.loc.gov/vocabulary/marcmuscomp/fg": "Fugues",
                "http://id.loc.gov/vocabulary/marcmuscomp/ct": "Cantatas",
                "http://id.loc.gov/vocabulary/marcmuscomp/mo": "Motets",
                "http://id.loc.gov/vocabulary/marcmuscomp/ft": "Fantasias",
                "http://id.loc.gov/vocabulary/marcmuscomp/or": "Oratorios",
                "http://id.loc.gov/vocabulary/marcmuscomp/su": "Suites",
                "http://id.loc.gov/vocabulary/marcmuscomp/pr": "Preludes",
                "http://id.loc.gov/vocabulary/marcmuscomp/ov": "Overtures",
                "http://id.loc.gov/vocabulary/marcgt/tre": "treaty",
                "http://id.loc.gov/vocabulary/marcmuscomp/mr": "Marches",
                "http://id.loc.gov/vocabulary/marcgt/pro": "programmed text",
                "http://id.loc.gov/vocabulary/marcmuscomp/dv": "Divertimentos, serenades, cassations, divertissements, and notturni",
                "http://id.loc.gov/vocabulary/marcmuscomp/rd": "Rondos",
                "https://id.nlm.nih.gov/mesh/D020492": "Periodical (MeSH)",
                "http://id.loc.gov/authorities/genreForms/gf2014026139.": "Periodical (LCGFT)",
                "http://id.loc.gov/vocabulary/marcmuscomp/rq": "Requiems",
                "http://id.loc.gov/vocabulary/marcmuscomp/cn": "Canons and rounds",
                "http://id.loc.gov/vocabulary/marcmuscomp/bt": "Ballets",
                "http://id.loc.gov/vocabulary/marcmuscomp/wz": "Waltzes",
                "http://id.loc.gov/vocabulary/marcmuscomp/fm": "Folk music",
                "http://id.loc.gov/vocabulary/marcmuscomp/md": "Madrigals",
                "http://id.loc.gov/vocabulary/marcgt/map": "map",
                "http://id.loc.gov/vocabulary/marcmuscomp/nc": "Nocturnes",
                "http://id.loc.gov/vocabulary/marcmuscomp/pt": "Part-songs",
                "http://id.loc.gov/vocabulary/marcmuscomp/hy": "Hymns",
                "http://id.loc.gov/vocabulary/marcmuscomp/pm": "Passion music",
                "http://id.loc.gov/vocabulary/marcgt/atl": "atlas",
                "http://id.loc.gov/vocabulary/marcmuscomp/mi": "Minuets",
                "http://id.loc.gov/vocabulary/marcmuscomp/tc": "Toccatas",
                "https://id.nlm.nih.gov/mesh/D020504": "Abstracts (MeSH)",
                "http://id.loc.gov/vocabulary/marcgt/stp": "standard or specification",
                "http://id.loc.gov/vocabulary/marcmuscomp/ts": "Trio-sonatas",
                "http://id.loc.gov/authorities/genreForms/gf2011026723": "Video recordings",
                "http://id.loc.gov/vocabulary/marcmuscomp/ch": "Chorales",
                "http://id.loc.gov/vocabulary/marcmuscomp/st": "Studies and exercises",
                "http://id.loc.gov/vocabulary/marcgt/com": "computer program",
                "http://id.loc.gov/vocabulary/marcmuscomp/cg": "Concerti grossi",
                "http://id.loc.gov/vocabulary/marcmuscomp/cp": "Chansons, polyphonic",
                "http://id.loc.gov/vocabulary/marcmuscomp/an": "Anthems",
                "http://id.loc.gov/vocabulary/marcmuscomp/po": "Polonaises",
                "http://id.loc.gov/vocabulary/marcmuscomp/cl": "Chorale preludes",
                "http://id.loc.gov/vocabulary/marcgt/vid": "videorecording",
                "http://id.loc.gov/authorities/genreForms/gf2014026909": "Librettos",
                "http://id.loc.gov/vocabulary/marcmuscomp/mz": "Mazurkas",
                "http://id.loc.gov/vocabulary/marcmuscomp/cc": "Chant, Christian",
                "http://id.loc.gov/vocabulary/marcmuscomp/ps": "Passacaglias",
                "http://id.loc.gov/vocabulary/marcgt/cgn": "comic or graphic novel",
                "http://id.loc.gov/vocabulary/marcmuscomp/sp": "Symphonic poems",
                "http://id.loc.gov/vocabulary/marcgt/num": "numeric data",
                "http://id.loc.gov/vocabulary/marcmuscomp/pp": "Popular music",
                "http://id.loc.gov/authorities/genreForms/gf2014026110.": "Humor",
                "http://id.loc.gov/vocabulary/marcmuscomp/cz": "Canzonas",
                "http://id.worldcat.org/fast/fst01411641": "Periodicals (FAST)",
                "http://id.loc.gov/vocabulary/marcmuscomp/pv": "Pavans"
            },
            "languages": {
                "eng": "English",
                "ger": "German",
                "fre": "French",
                "spa": "Spanish",
                "rus": "Russian",
                "chi": "Chinese",
                "jpn": "Japanese",
                "ita": "Italian",
                "por": "Portuguese",
                "lat": "Latin",
                "ara": "Arabic",
                "und": "Undetermined",
                "dut": "Dutch",
                "pol": "Polish",
                "swe": "Swedish",
                "heb": "Hebrew",
                "dan": "Danish",
                "kor": "Korean",
                "cze": "Czech",
                "hin": "Hindi",
                "ind": "Indonesian",
                "hun": "Hungarian",
                "mul": "Multiple languages",
                "nor": "Norwegian",
                "tur": "Turkish",
                "scr": "Croatian",
                "zxx": "No linguistic content",
                "urd": "Urdu",
                "tha": "Thai",
                "gre": "Greek, Modern (1453-)",
                "per": "Persian",
                "grc": "Greek, Ancient (to 1453)",
                "san": "Sanskrit",
                "tam": "Tamil",
                "ukr": "Ukrainian",
                "bul": "Bulgarian",
                "scc": "Serbian",
                "rum": "Romanian",
                "ben": "Bengali",
                "vie": "Vietnamese",
                "fin": "Finnish",
                "arm": "Armenian",
                "cat": "Catalan",
                "slo": "Slovak",
                "slv": "Slovenian",
                "yid": "Yiddish",
                "mar": "Marathi",
                "may": "Malay",
                "pan": "Panjabi",
                "afr": "Afrikaans",
                "tel": "Telugu",
                "ota": "Turkish, Ottoman",
                "tib": "Tibetan",
                "ice": "Icelandic",
                "mal": "Malayalam",
                "est": "Estonian",
                "bel": "Belarusian",
                "lit": "Lithuanian",
                "mac": "Macedonian",
                "lav": "Latvian",
                "nep": "Nepali",
                "uzb": "Uzbek",
                "wel": "Welsh",
                "kan": "Kannada",
                "geo": "Georgian",
                "guj": "Gujarati",
                "snh": "Sinhalese",
                "srp": "Serbian",
                "hrv": "Croatian (Discontinued Code)",
                "bur": "Burmese",
                "pli": "Pali",
                "kaz": "Kazakh",
                "tgl": "Tagalog",
                "aze": "Azerbaijani",
                "mon": "Mongolian",
                "jav": "Javanese",
                "iri": "Irish (Discontinued Code)",
                "hau": "Hausa",
                "fro": "French, Old (ca. 842-1300)",
                "swa": "Swahili",
                "map": "Austronesian (Other)",
                "gmh": "German, Middle High (ca. 1050-1500)",
                "syr": "Syriac, Modern",
                "raj": "Rajasthani",
                "ori": "Oriya",
                "alb": "Albanian",
                "sla": "Slavic (Other)",
                "enm": "English, Middle (1100-1500)",
                "arc": "Aramaic",
                "pra": "Prakrit languages",
                "sin": "Sinhalese",
                "chu": "Church Slavic",
                "ang": "English, Old (ca. 450-1100)",
                "gle": "Irish",
                "nic": "Niger-Kordofanian (Other)",
                "kir": "Kyrgyz",
                "frm": "French, Middle (ca. 1300-1600)",
                "tut": "Altaic (Other)",
                "roa": "Romance (Other)",
                "tag": "Tagalog (Discontinued Code)",
                "inc": "Indic (Other)",
                "tat": "Tatar",
                "myn": "Mayan languages",
                "tuk": "Turkmen",
                "sun": "Sundanese",
                "baq": "Basque",
                "sai": "South American Indian (Other)",
                "mai": "Maithili",
                "egy": "Egyptian",
                "akk": "Akkadian",
                "sit": "Sino-Tibetan (Other)",
                "que": "Quechua",
                "pro": "Provençal (to 1500)",
                "cop": "Coptic",
                "int": "Interlingua (International Auxiliary Language Association) (Discontinued Code)",
                "yor": "Yoruba",
                "paa": "Papuan (Other)",
                "bra": "Braj",
                "new": "Newari",
                "pus": "Pushto",
                "amh": "Amharic",
                "bos": "Bosnian",
                "rom": "Romani",
                "gem": "Germanic (Other)",
                "fiu": "Finno-Ugrian (Other)",
                "mol": "Moldavian (Discontinued Code)",
                "roh": "Raeto-Romance",
                "fri": "Frisian (Discontinued Code)",
                "lao": "Lao",
                "snd": "Sindhi",
                "wen": "Sorbian (Other)",
                "nah": "Nahuatl",
                "bak": "Bashkir",
                "pal": "Pahlavi",
                "asm": "Assamese",
                "glg": "Galician",
                "cai": "Central American Indian (Other)",
                "gag": "Galician (Discontinued Code)",
                "uig": "Uighur",
                "tgk": "Tajik",
                "gae": "Scottish Gaelix (Discontinued Code)",
                "khm": "Khmer",
                "esp": "Esperanto (Discontinued Code)",
                "epo": "Esperanto",
                "gez": "Ethiopic",
                "bho": "Bhojpuri",
                "gla": "Scottish Gaelic",
                "kas": "Kashmiri",
                "som": "Somali",
                "nai": "North American Indian (Other)",
                "fry": "Frisian",
                "crp": "Creoles and Pidgins (Other)",
                "zul": "Zulu",
                "taj": "Tajik (Discontinued Code)",
                "mao": "Maori",
                "eth": "Ethiopic (Discontinued Code)",
                "tah": "Tahitian",
                "mis": "Miscellaneous languages",
                "lan": "Occitan (post 1500) (Discontinued Code)",
                "haw": "Hawaiian",
                "sna": "Shona",
                "cpf": "Creoles and Pidgins, French-based (Other)",
                "cau": "Caucasian (Other)",
                "jrb": "Judeo-Arabic",
                "kur": "Kurdish",
                "sot": "Sotho",
                "awa": "Awadhi",
                "bre": "Breton",
                "oci": "Occitan (post-1500)",
                "ban": "Balinese",
                "ibo": "Igbo",
                "lad": "Ladino",
                "mlg": "Malagasy",
                "goh": "German, Old High (ca. 750-1050)",
                "tsn": "Tswana",
                "sux": "Sumerian",
                "ber": "Berber (Other)",
                "doi": "Dogri",
                "gua": "Guarani (Discontinued Code)",
                "bnt": "Bantu (Other)",
                "esk": "Eskimo languages (Discontinued Code)",
                "kin": "Kinyarwanda",
                "mni": "Manipuri",
                "xho": "Xhosa",
                "ssa": "Nilo-Saharan (Other)",
                "aym": "Aymara",
                "ful": "Fula",
                "dum": "Dutch, Middle (ca. 1050-1350)",
                "tar": "Tatar (Discontinued Code)"
            },
            "digitization_agent_code": {
                "google": "Google (google)",
                "ia": "Internet Archive (ia)",
                "lit-dlps-dc": "Library IT, Digital Library Production Service, Digital Conversion (lit-dlps-dc)",
                "cornell-ms": "Cornell University (with support from Microsoft) (cornell-ms)",
                "yale": "Yale University (yale)",
                "berkeley": "University of California, Berkeley (berkeley)",
                "getty": "Getty Research Institute (getty)",
                "uiuc": "University of Illinois at Urbana-Champaign (uiuc)",
                "tamu": "Texas A&M (tamu)",
                "cornell": "Cornell University (cornell)",
                "northwestern": "Northwestern University (northwestern)",
                "yale2": "Yale University (yale2)",
                "borndigital": "Born Digital (placeholder) (borndigital)",
                "nnc": "Columbia University (nnc)",
                "geu": "Emory University (geu)",
                "princeton": "Princeton University (princeton)",
                "mou": "University of Missouri-Columbia (mou)",
                "mcgill": "McGill University (mcgill)",
                "umd": "University of Maryland (umd)",
                "harvard": "Harvard University (harvard)",
                "ucsd": "University of California, San Diego (ucsd)",
                "wau": "University of Washington (wau)",
                "aub": "American University of Beirut (aub)",
                "uf": "State University System of Florida (uf)",
                "bc": "Boston College (bc)",
                "ucla": "University of California, Los Angeles (ucla)",
                "clark": "Clark Art Institute (clark)",
                "ucm": "Universidad Complutense de Madrid (ucm)",
                "upenn": "University of Pennsylvania (upenn)",
                "chtanc": "National Central Library of Taiwan (chtanc)",
                "uq": "The University of Queensland (uq)"
            },
            "format": {
                "http://id.loc.gov/ontologies/bibframe/Text": "Text",
                "http://id.loc.gov/ontologies/bibframe/NotatedMusic": "NotatedMusic",
                "http://id.loc.gov/ontologies/bibframe/Cartography": "Cartography",
                "http://id.loc.gov/ontologies/bibframe/Text http://id.loc.gov/ontologies/bibframe/Audio": "Text Audio",
                "http://id.loc.gov/ontologies/bibframe/NotatedMusic http://id.loc.gov/ontologies/bibframe/Audio": "NotatedMusic Audio",
                "http://id.loc.gov/ontologies/bibframe/MixedMaterial": "MixedMaterial",
                "http://id.loc.gov/ontologies/bibframe/StillImage": "StillImage",
                "http://id.loc.gov/ontologies/bibframe/Audio": "Audio"
            },
            "htsource": {
                "MIU": "University of Michigan (MIU)",
                "NRLF": "UC Northern Regional Library Facility (NRLF)",
                "HVD": "Harvard University (HVD)",
                "UIUC": "University of Illinois at Urbana-Champaign (UIUC)",
                "UMN": "University of Minnesota (UMN)",
                "UVA": "University of Virginia (UVA)",
                "COO": "Cornell University (COO)",
                "WU": "University of Wisconsin (WU)",
                "INU": "Indiana University (INU)",
                "UCSD": "University of California, San Diego (UCSD)",
                "TXU": "University of Texas, Austin (TXU)",
                "PST": "Pennsylvania State University (PST)",
                "NYP": "New York Public Library (NYP)",
                "OSU": "The Ohio State University (OSU)",
                "NJP": "Princeton University (NJP)",
                "UCLA": "University of California, Los Angeles (UCLA)",
                "UCSC": "University of California, Santa Cruz (UCSC)",
                "UCBK": "University of California, Berkeley (UCBK)",
                "NWU": "Northwestern University (NWU)",
                "SRLF": "UC Southern Regional Library Facility (SRLF)",
                "UCR": "University of California, Riverside (UCR)",
                "CHI": "University of Chicago (CHI)",
                "UCM": "Universidad de Complutense de Madrid (UCM)",
                "ILOC": "Library of Congress (ILOC)",
                "NNC": "Columbia University (NNC)",
                "ISRLF": "UC Southern Regional Library Facility (ISRLF)",
                "KEIO": "Keio University (KEIO)",
                "IUIUC": "University of Illinois at Urbana-Champaign (IUIUC)",
                "INRLF": "INRLF",
                "MSU": "Michigan State University (MSU)",
                "AEU": "University of Alberta (AEU)",
                "IAU": "University of Iowa (IAU)",
                "GWLA": "GWLA",
                "GRI": "GRI",
                "PUR": "Purdue University (PUR)",
                "UCD": "University of California, Davis (UCD)",
                "IUNC": "University of North Carolina at Chapel Hill (IUNC)",
                "INNC": "Columbia University (INNC)",
                "YALE": "Yale University (YALE)",
                "MU": "University of Massachusetts Amherst (MU)",
                "IDUKE": "Duke University (IDUKE)",
                "UCI": "UCI",
                "IUFL": "University of Florida (IUFL)",
                "UIUCL": "University of Illinois at Urbana-Champaign (UIUCL)",
                "IBC": "Boston College (IBC)",
                "TXCM": "Texas A&M University (TXCM)",
                "UMDB": "UMDB",
                "CTU": "University of Connecticut (CTU)",
                "IUCD": "University of California, Davis (IUCD)",
                "INCSU": "North Carolina State University (INCSU)",
                "UCSF": "University of California, San Francisco (UCSF)",
                "GEU": "Emory University (GEU)",
                "IPST": "Pennsylvania State University (IPST)",
                "MOU": "University of Missouri, Columbia (MOU)",
                "NCWSW": "Wake Forest University (NCWSW)",
                "MMET": "Tufts University (MMET)",
                "QMM": "McGill University (QMM)",
                "UCSB": "University of California, Santa Barbara (UCSB)",
                "UMLAW": "UMLAW",
                "MDU": "University of Maryland (MDU)",
                "WAU": "University of Washington (WAU)"
                "UMBUS": "UMBUS",
                "IUCLA": "University of California, Los Angeles (IUCLA)",
                "LEBAU": "American University of Beirut (LEBAU)",
                "UFDC": "University of Florida (UFDC)",
                "AZTES": "Arizona State University (AZTES)",
                "FMU": "University of Miami (FMU)",
                "MWICA": "Clark Art Institute (MWICA)",
                "PU": "University of Pennsylvania, Van Pelt-Dietrich Library (PU)",
                "AUBRU": "University of Queensland, Saint Lucia (AUBRU)"
            }
        }
        replace_columns = {}
        print(df.columns)
        for facet_key in map_to_human_readable.keys():
            if facet_key in df.columns:
                replace_columns[facet_key] = map_to_human_readable[facet_key]

        if replace_columns:
            df = df.replace(replace_columns)
        
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
