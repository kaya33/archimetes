"""
Copyright (C) 2015 Baifendian Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from django.db import models
from mongoengine import *

class Task(models.Model):
    class Meta:
        permissions = (
            ('admin_task', 'Can admin tasks'),
            ('guest_task', 'Can guest tasks'),
        )

class UserTag(Document):
    user_id = StringField(max_length=100,required=True)
    user_tag = DictField()
