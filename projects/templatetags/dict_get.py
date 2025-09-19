# dict_get.py
from django import template
register = template.Library()

@register.filter
def get(dict_obj, key):
    return dict_obj.get(key)