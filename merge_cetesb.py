from os import replace
import pandas as pd
import os
from multiprocessing import Pool


directory_path = 'data\cetesb'


def concat_csv(station_id):

    list_files = os.listdir(directory_path)
    df = pd.DataFrame()
    station_id = station_id.replace('/', '-')
    print(f'{station_id}')

    columns = [
        'Hora',
        'Data',
        'station_name',
        'MP10 µg/m³|Média  horária',
        'MP10 µg/m³|Média  24 h',
        'MP10 µg/m³|Índice /  Qualidade',
        'MP2.5 µg/m³|Média  horária',
        'MP2.5 µg/m³|Média  24 h',
        'MP2.5 µg/m³|Índice /  Qualidade'
    ]
    for file in list_files:
        if file.startswith(station_id):
            print(f'{file}')
            df_edit = pd.read_csv(f'{directory_path}\{file}')
            try:
                df = df.append(df_edit[columns])
                print(f'{station_id}|{df.shape}')
            except:
                pass
    if df.shape[0] > 0:
        df.to_csv(f'data/cetesb_{station_id}.csv', index=False)
    return


def pool_handler(stations):
    p = Pool(100)
    p.map(concat_csv, stations)


if __name__ == '__main__':
    df_stations = pd.read_csv('data\stations.csv')
    pool_handler(df_stations['stations'].values)
