import unittest
import sys

sys.path.append('./archimedes')

from api.user_tag import UserProfile
up = UserProfile('chaoge', 0)
up.connect()

class TestMongo(unittest.TestCase):

    def test_init(self):
        off_tag_data = up.read_tag('RecommendationUserTagsOffline', {'_id':"0025bb8623734a798537991546a0d47c"})
        print off_tag_data

