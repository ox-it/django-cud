import itertools
import Queue
import logging
import threading
from xml.parsers import expat

from django.conf import settings
from django.core.management.base import NoArgsCommand
from django.db.models.loading import get_model
from django.contrib.auth.models import User
import requests
from requests_kerberos import HTTPKerberosAuth

from ...config import CUD_ID_FIELD, CUD_FIELD_MAPPING

logger = logging.getLogger(__name__)

class CUDParser(object):
    def __init__(self, stream):
        self._stream = stream
        self._queue = Queue.Queue()

    class CUDHandler(object):
        def __init__(self, queue):
            self._queue = queue
            self._names = []
            self._content = None

        def start_element(self, name, attrs):
            self._names.append(name)
            self._content = []
            if name == 'cudSubject':
                self._attributes = {}
            elif name == 'cudAttribute':
                self._attribute = None
            elif name == 'value':
                self._value_class = attrs['class']
                if attrs['class'] == 'string':
                    self._attribute = None
                elif attrs['class'] == 'list':
                    self._attribute = []
                else:
                    raise AssertionError
            elif name in ('cudSubjects', 'attributes', 'name', 'string'):
                pass
            else:
                raise AssertionError(name)

        def end_element(self, name):
            assert name == self._names.pop()
            content = ''.join(self._content)
            
            if name == 'name':
                self._attribute_name = content
            elif name == 'cudAttribute':
                self._attributes[self._attribute_name] = self._attribute
            elif name == 'cudSubject':
                self._queue.put(self._attributes)
            elif name == 'value': 
                if self._value_class == 'string':
                    self._attribute = content
            elif name == 'string':
                self._attribute.append(content)
            elif name in ('cudSubjects', 'attributes'):
                pass
            else:
                raise AssertionError(name)
            #print name

        def char_data(self, data):
            self._content.append(data)
    
    def __iter__(self):
        self.parse_thread = threading.Thread(target=self._parse)
        self.parse_thread.run()
        
        while True:
            item = self._queue.get()
            if item is None:
                break
            yield item

    def _parse(self):
        parser = expat.ParserCreate(namespace_separator=' ')
        handler = self.CUDHandler(self._queue)
        parser.StartElementHandler = handler.start_element
        parser.EndElementHandler = handler.end_element
        parser.CharacterDataHandler = handler.char_data

        try:
            parser.ParseFile(self._stream)
        except Exception:
            logger.exception("Failed to parse stream")
        finally:
            self._queue.put(None)

def group_n(it, n):
    it = iter(it) # Just in case we have an iterable, not an iterator
    while True:
        out = list(itertools.islice(it, 0, n, 1))
        if out:
            yield out
        else:
            break

class Command(NoArgsCommand):
    cud_endpoint = "https://ws.cud.ox.ac.uk/cudws/rest/search"
    
    def handle_noargs(self, **kwargs):
        Profile = get_model(*settings.AUTH_PROFILE_MODULE.rsplit('.', 1))
        if not CUD_ID_FIELD in [f.name for f in Profile._meta.fields]:
            raise AssertionError("Profile model must have a {0} field.".format(CUD_ID_FIELD))
        
        response = requests.get(self.cud_endpoint,
                                params={'q': r'cud\:cas\:lastname:Dutton',
                                        'format': 'xml'},
                                auth=HTTPKerberosAuth(),
                                stream=True)

        subjects_iter = CUDParser(response.raw)
        for subjects in group_n(subjects_iter, 5):
            subjects = dict((s['cud:cas:cudid'], s) for s in subjects)
            cud_ids = set(subjects)
            profiles = Profile.objects.filter(**{CUD_ID_FIELD+'__in': cud_ids})
            profiles = dict((getattr(p, CUD_ID_FIELD), p) for p in profiles)
            for cud_id in cud_ids:
                if cud_id not in profiles:
                    profiles[cud_id] = Profile(cud_id=cud_id)
                    profiles[cud_id].user, created = User.objects.get_or_create(username=cud_id[:30])
            
            for cud_id in cud_ids:
                subject, profile = subjects.get(cud_id), profiles.get(cud_id)
                changed = False
                for source, getter, setter in CUD_FIELD_MAPPING:
                    value, old_value = subject.get(source), getter(profile)
                    if not value:
                        value = ''
                    if value != old_value:
                        setter(profile, value)
                        changed = True
                if changed:
                    profile.save()
                        
            