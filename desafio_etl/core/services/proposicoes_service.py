import requests

import os
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = os.getenv('API_BASE_URL')

class ProposicoesService:
    def __init__(self, max_results_per_page: int = 100, format :str = "json", year:str = "2023", ordenate:str = "3") -> None:
        
        self.base_url = API_BASE_URL
        self.results_per_page = max_results_per_page
        self.format = format
        self.year = year
        self.ordenate = ordenate
        
    def __build_endpoint(self, page:str):
        
        return f"{self.base_url}?tp={self.results_per_page}&formato={self.format}&ano={self.year}&ord={self.ordenate}?p{page}"

    
    def request_data(self):
        
        page_index = 1
        data = []
        ocurrency_number = int(requests.get(self.endpoint).json()["resultado"]["noOcorrencias"])
        total_pages = ocurrency_number / self.results_per_page if ocurrency_number % self.results_per_page == 0 else ocurrency_number / self.results_per_page + 1

        while (page_index <= total_pages):
            json_response = requests.get(self.__build_endpoint(page_index)).json()
            page_index =+ 1
            
            data.append(json_response)
            
    def __call__(self):
        return self.request_data()
        
    