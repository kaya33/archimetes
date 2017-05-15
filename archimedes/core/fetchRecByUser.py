#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = 'xujiang@baixing.com'

from archimedes import logger

log = logger.getLogger(__name__)


def fetchRecByUserProfileAndItemCF():
    res = RecResponse()
    res.status = responseType.OK
    res.err_str = ""
    res.data = []

