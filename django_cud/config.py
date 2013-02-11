from django.conf import settings

CUD_ID_FIELD = getattr(settings, 'CUD_ID_FIELD', 'cud_id')

CUD_FIELD_MAPPING = {'cud:cas:fullname': 'name',
                     'cud:cas:firstname': 'user.first_name',
                     'cud:cas:lastname': 'user.last_name',
                     'cud:cas:title': 'title',
                     'cud:cas:oxford_email': 'user.email',
                     'cud:cas:internel_tel': 'work_phone',
                     'cud:cas:sso_username': 'sso_username',
                     'cud:cas:barcode': None}#'card_number'}
CUD_FIELD_MAPPING.update(getattr(settings, 'CUD_FIELD_MAPPING', {}))

def field_mapper(cud_attribute_name, name):
    if name is None:
        return
    parts = name.split('.')
    ps, p = parts[:-1], parts[-1]
    def getter(obj):
        for part in parts:
            obj = getattr(obj, part)
        return obj
    def setter(obj, value):
        for part in ps:
            obj = getattr(obj, part)
        setattr(obj, p, value)
    return cud_attribute_name, getter, setter

CUD_FIELD_MAPPING = filter(None, [field_mapper(*mapping) for mapping in CUD_FIELD_MAPPING.iteritems()])

CUD_NEW_PROFILE = getattr(settings, 'CUD_NEW_PROFILE', None)

        

def cud_is_active(user):
    pass
