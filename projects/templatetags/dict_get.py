# dict_get.py
from django import template
register = template.Library()

@register.filter
def get(dict_obj, key):
    return dict_obj.get(key)

@register.filter
def dict_get(d, key):
    """Safely get dict value by key."""
    if isinstance(d, dict):
        return d.get(key)
    return None