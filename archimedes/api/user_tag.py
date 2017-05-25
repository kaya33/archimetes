import logging
from mongo_base import Mongo


class UserProfile(Mongo):

    def read_tag(self, collect_name, search_json={}, top_category='', category='', top=5):
        result = self.read(collect_name, search_json)
        result_dict = {}
        try:
            for k1, v1 in result.next()['tags'].items():

                if k1 != top_category and top_category != '':
                    continue

                result_dict.setdefault(k1, {})
                for k2, v2 in v1.items():

                    if k2 != category and category != '':
                        continue

                    result_dict[k1].setdefault(k2, {})
                    for k3, v3 in v2.items():

                        result_dict[k1][k2].setdefault(k3, {})

                        for k4, v4 in v3.items():
                            k4 = k4.replace('```', '.')
                            k4 = k4.replace('%^&', '$')
                            result_dict[k1][k2][k3][k4] = sorted(v4.items(), key=lambda d: d[1], reverse=True)[:top]

        except KeyError as e:
            logging.error(e)
            return {}

        return result_dict


def test():
    a = UserProfile('chaoge')
    a.connect()
    user_id = '748418c365704429a4e8645dddb6e995'
    # print a.read_tag('RecommendationUserTagsOffline', {})
    # print a.read_tag('RecommendationUserTagsOffline', {'user_id':user_id})

# test()
