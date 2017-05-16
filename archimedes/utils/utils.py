# -*- coding: utf-8 -*-

import sys
import traceback


def import_class(import_str):
    """Returns a class from a string including module and class."""
    mod_str, _sep, class_str = import_str.rpartition('.')
    __import__(mod_str)
    try:
        return getattr(sys.modules[mod_str], class_str)
    except AttributeError:
        raise ImportError('Class %s cannot be found (%s)' %
                          (class_str,
                           traceback.format_exception(*sys.exc_info())))


def get_module(import_str):
    module_name = import_str.rpartition('.')[-1]
    if not module_name:
        raise("import_str:%s not regular", import_str)
    return module_name