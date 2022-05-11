import json

class Transform:

    @staticmethod
    def restructure_response(response, key):
        if key in response:
            ts = response.get(key)
            array = []
            for date in ts:
                array.append(ts.get(date))
            response[key] = array
        return response

