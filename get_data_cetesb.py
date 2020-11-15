from multiprocessing import Pool
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime, timedelta


def get_html(date_to_get, station_id):
    url = "https://sistemasinter.cetesb.sp.gov.br/Ar/php/ar_dados_horarios_resultado.php"

    payload = {
        'texData': f'{date_to_get}',
        'selEst': station_id
    }
    files = []
    headers = {}

    response = requests.request(
        "POST", url, headers=headers, data=payload, files=files)
    response_txt = response.text.encode('utf8')
    soup = BeautifulSoup(response_txt, features="lxml")

    htmltable = soup.find('table', {'class': 'font01'})

    return htmltable


def list_data(dates_list):
    df_stations = pd.read_csv('data\stations.csv')
    for station_cod in (df_stations['stations'].values):
        # for date in dates_list:
        print(f'Get data in {dates_list[0]} to station {station_cod}')
        df = pd.read_html(str(get_html(dates_list[0], station_cod)))

        df_data = pd.DataFrame(df[1].rename(columns={0: df[1][0][0]}).drop(
            [0]).reset_index(drop=True))
        df_data['Data'] = str(dates_list[1])
        df_data['station_name'] = str(station_cod.split('/')[1])

        if len(df) >= 6:
            for table_number in range(2, len(df)):
                for column_number in list(df[table_number].columns):
                    column_name = f'{str(df[table_number][column_number][0])}|{str(df[table_number][column_number][1])}'
                    df_data[column_name] = df[table_number][column_number].drop(
                        [0, 1]).reset_index(drop=True).replace('--', np.nan)

            name_file = f'data/cetesb/{station_cod.replace("/","-")}_{dates_list[1].replace("-","_")}.csv'
            df_data.to_csv(name_file, index=False)
            print(f'Saved data: {name_file}')
    return


today = datetime.utcnow()
dates_list = tuple(
    [
        datetime.strftime(today - timedelta(days=days_back), "%d/%m/%Y"),
        datetime.strftime(today - timedelta(days=days_back), "%Y-%m-%d")
    ]
    for days_back in range(0, 3000)
)
# df_stations = pd.read_csv('data\stations.csv')
# print(df_stations['stations'].values)
# print(dates_list)


def pool_handler():
    p = Pool(50)
    p.map(list_data, dates_list)


if __name__ == '__main__':
    pool_handler()
