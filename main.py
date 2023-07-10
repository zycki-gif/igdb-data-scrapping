import datetime
import json
import os
import requests
from dotenv import load_dotenv
import argparse

load_dotenv()


def get_twitch_token(client_secret, client_id):
    """
     Retrieves an access token from Twitch API.

     Args:
         client_secret (str): Twitch API client secret.
         client_id (str): Twitch API client ID.

     Returns:
         str: Access token.

     """
    params = {
        "client_secret": client_secret,
        "client_id": client_id,
        "grant_type": "client_credentials",
    }

    url = "https://id.twitch.tv/oauth2/token"
    resp = requests.post(url, params=params)
    data = resp.json()

    token = data['access_token']
    return token


class Ingestor:
    """
    Class responsible for ingesting data from the IGDB API.

    Args:
        token (str): Access token for IGDB API.
        client_id (str): Twitch API client ID.
        delay (int): Delay in days for data retrieval.
        path (str): Path to save the retrieved data.

    """

    def __init__(self, token, client_id, delay, path) -> None:
        self.headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
        }
        self.base_url = 'https://api.igdb.com/v4/{sufix}'
        self.delay = delay
        self.delay_timestamp = datetime.datetime.now() - datetime.timedelta(days=delay)
        self.delay_timestamp = int(self.delay_timestamp.timestamp())
        self.path = path

    def get_data(self, sufix, params={}):

        url = self.base_url.format(sufix=sufix)
        data = requests.get(url, headers=self.headers, params=params)
        return data.json()

    def save_data(self, data, sufix):

        name = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
        with open(os.path.join(f'{self.path}/{sufix}/{name}.json'), 'w') as open_file:
            json.dump(data, open_file)
        return True

    def get_and_save(self, sufix, params):
        data = self.get_data(sufix, params)
        self.save_data(data, sufix)
        return data

    def process(self, sufix, **params):
        default = {
            'fields': '*',
            'limit': 500,
            'offset': 0,
            'order': 'updated_at:desc',
        }

        default.update(params)

        print("Iniciando loop...")
        while True:

            print("Obtendo dados...")
            data = self.get_and_save(sufix, default)
            try:
                updated_timestamp = int(data[-1]['updated_at'])
                print(updated_timestamp, "... Ok.")

            except KeyError as err:
                print(err)
                print(data[-1].keys())
                updated_timestamp = int(
                    datetime.datetime.now().timestamp()) - 100000

            if len(data) < 500 or updated_timestamp < self.delay_timestamp:
                print("Finalizando loop...")
                return True

            default['offset'] += default['limit']


def collect(endpoint, delay, path, **params):
    client_secret = os.environ['CLIENT_SECRET']
    client_id = os.environ["CLIENT_ID"]

    if not os.path.exists(f'{path}/{endpoint}'):
        os.mkdir(f'{path}/{endpoint}')

    print('Obtendo token da twitch...')
    token = get_twitch_token(client_secret, client_id)
    print('Ok.\n')

    print('Criando classe de ingestão...')
    ingestor = Ingestor(token, client_id, delay, path)
    print('Ok.\n')

    print('Iniciando o processo...')
    ingestor.process(endpoint, **params)
    print('Ok.\n')


def main():
    parser = argparse.ArgumentParser(
        description='Script de coleta de dados da API da IGDB')
    parser.add_argument('--endpoint', type=str, help='Endpoint da API da IGDB')
    parser.add_argument('--mode', type=str,
                        help='Modo de coleta (all ou specific)')
    parser.add_argument('--delay', type=int,
                        help='Atraso em dias para a coleta')
    args = parser.parse_args()

    path = './json'
    if args.mode == 'collect':
        collect(args.endpoint, delay=args.delay, path=path)
    elif args.mode == 'all':
        collect(args.endpoint, delay=args.delay, path=path)


if __name__ == '__main__':
    main()
