from typing import Dict, List
import requests


class ApiReader:
    def __init__(self,
                 url: str,
                 login: str,
                 cohort: str,
                 key: str) -> None:
        self.url = url
        self.login = login
        self.cohort = cohort
        self.key = key
        self.headers = {
            'X-Nickname': login,
            'X-Cohort': cohort,
            'X-API-KEY': key
        }

    def get(self,
            offset: str,
            sort_field: str = 'id',
            sort_direction: str = 'asc',
            limit: str = '50') -> Dict:
        params = {
            "sort_field": sort_field,
            "offset": offset,
            "sort_direction": sort_direction,
            "limit": limit
        }
        return requests.get(
            url=self.url,
            headers=self.headers,
            params=params
        ).json()  # any runtime exception allowed
