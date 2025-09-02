# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#    Benjamin Arfa - improvements
#
from django import template

register = template.Library()

@register.filter
def get_range(value):
    """
    Filter - returns a list containing range made from given value
    Usage (in template):
    {% for i in max_rows|get_range %}
    """
    return range(value)

@register.filter
def get_item(lst, i):
    """
    Filter - returns the item at index i from the list
    Usage (in template):
    {{ my_list|get_item:index }}
    """
    try:
        return lst[i]
    except:
        return None

@register.filter
def get_attr(obj, attr):
    """
    Filter - returns the attribute value from an object
    Usage (in template):
    {{ my_object|get_attr:"attribute_name" }}
    """
    try:
        return getattr(obj, attr)
    except:
        return None

@register.filter
def get_dict_item(dictionary, key):
    """
    Filter - returns the dictionary value for a given key
    Usage (in template):
    {{ my_dict|get_dict_item:key }}
    """
    try:
        return dictionary.get(key)
    except:
        return None 