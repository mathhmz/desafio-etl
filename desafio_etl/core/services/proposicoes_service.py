import requests
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()
API_BASE_URL = os.getenv('API_BASE_URL')

class ProposicoesService:
    """
    Serviço para realizar requisições à API de proposições.

    Atributos:
        base_url (str): URL base da API.
        results_per_page (int): Número máximo de resultados por página.
        format (str): Formato dos dados de resposta.
        year (str): Ano das proposições.
        ordenate (str): Ordem dos resultados.

    Métodos:
        __init__(max_results_per_page, format, year, ordenate): Inicializa o serviço com os parâmetros fornecidos.
        __build_endpoint(page): Constrói o endpoint da API para a página especificada.
        request_data(): Realiza requisições à API e retorna os dados.
        __call__(): Invoca o método request_data e retorna os dados.
    """
    def __init__(self, max_results_per_page: int = 100, format: str = "json", year: str = "2023", ordenate: str = "3") -> None:
        """
        Inicializa o serviço com os parâmetros fornecidos.

        Args:
            max_results_per_page (int): Número máximo de resultados por página. Padrão é 100.
            format (str): Formato dos dados de resposta. Padrão é "json".
            year (str): Ano das proposições. Padrão é "2023".
            ordenate (str): Ordem dos resultados. Padrão é "3".
        """
        self.base_url = API_BASE_URL
        self.results_per_page = max_results_per_page
        self.format = format
        self.year = year
        self.ordenate = ordenate

    def __build_endpoint(self, page: int) -> str:
        """
        Constrói o endpoint da API para a página especificada.

        Args:
            page (int): Número da página.

        Returns:
            str: URL completa para a requisição da página.
        """
        return f"{self.base_url}?tp={self.results_per_page}&formato={self.format}&ano={self.year}&ord={self.ordenate}&p={page}"

    def request_data(self) -> list[dict]:
        """
        Realiza requisições à API e retorna os dados.

        Returns:
            list[dict]: Lista de dicionários com os dados das proposições.
        """
        page_index = 1
        data = []

        response = requests.get(self.__build_endpoint(page_index))
        response.raise_for_status()  # Levanta uma exceção para respostas com erro
        ocurrency_number = int(response.json()["resultado"]["noOcorrencias"])

        total_pages = (ocurrency_number // self.results_per_page) + (1 if ocurrency_number % self.results_per_page else 0)

        while page_index <= total_pages:
            response = requests.get(self.__build_endpoint(page_index))
            response.raise_for_status()  # Levanta uma exceção para respostas com erro
            data.append(response.json())
            page_index += 1

        return data

    def __call__(self) -> list[dict]:
        """
        Invoca o método request_data e retorna os dados.

        Returns:
            list[dict]: Lista de dicionários com os dados das proposições.
        """
        return self.request_data()