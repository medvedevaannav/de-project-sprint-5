from datetime import datetime
from typing import Dict, List
import requests
import json


class DeliveryReader:
    def __init__(self, base_url: str, headers: Dict, log) -> None:
        self.base_url = base_url
        self.headers = headers
        self.tbl_name = '/deliveries'
        self.log = log

    def get_deliveries(self) -> List[Dict]:

        params = {
           # 'from': load_threshold.strftime("%Y-%m-%d %H:%M:%S"),
            'sort_field': 'id', 
            'sort_direction': 'asc',
            'offset': 0
            }

        result_list = []

        r = requests.get(self.base_url + self.tbl_name, headers=self.headers, params=params)
        while r.text != '[]':
            self.log.info(str(r.content))
            self.log.info(str(r.url))
            response_list = json.loads(r.content)

            for delivery_dict in response_list:
                result_list.append({'object_id': delivery_dict['delivery_id'], 
                                    'object_value': delivery_dict,
                                    'update_ts': datetime.fromisoformat(delivery_dict['delivery_ts'])})
                
            params['offset'] += len(response_list)

            r = requests.get(self.base_url + self.tbl_name, headers=self.headers, params=params)

        return result_list
