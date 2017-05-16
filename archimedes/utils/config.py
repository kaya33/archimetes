# -*- coding: utf-8 -*-

import ConfigParser
from ConfigParser import NoSectionError, NoOptionError


class Config(object):

    def __init__(self, file_name=None):
        self.cf = ConfigParser.ConfigParser()
        if file_name:
            self.cf.read(file_name)

    def set(self, section, option, value):
        if not self.cf.has_section(section):
            self.cf.add_section(section)
        self.cf.set(section, option, value)

    def get(self, section, option, default="", required=False):
        try:
            value = self.cf.get(section, option)
        except (NoSectionError, NoOptionError) as e:
            if required:
                raise e
            value = default
        return value

    def _get(self, section, option, convert, default=None, required=False):
        return convert(self.get(section, option, default, required))

    def getint(self, section, option, default=0, required=False):
        return self._get(section, option, int, default, required)

    def getfloat(self, section, option, default=0.0, required=False):
        return self._get(section, option, float, default, required)

    _boolean_states = {'1': True, 'yes': True, 'true': True, 'on': True,
                       '0': False, 'no': False, 'false': False, 'off': False}

    def getboolean(self, section, option, default=False, required=False):
        v = self.get(section, option, default, required)
        if v.lower() not in self._boolean_states:
            raise ValueError, 'Not a boolean: %s' % v
        return self._boolean_states[v.lower()]