import psycopg2 as ps
import pandas as pd
import numpy as np

import requests
import json
import io


def get_tourn_result(tourn_id: int):
    """
    Получаем данные конкретного турнира по API и переводим в плоскую таблицу.
    Можно забрать больше данных, если будет желание, читайте документацию
    Т.к. данные достаточно разные, результат выводим в три датафрейма, доступные для джойна в один
    
    Параметры:
    tourn_id - уникальный id турнира

    Возвращаем следующие датафреймы: 
    - torun_df - небольшой, только данные по турниру (результаты турнира, рейтинг и всякое такое)
    - question_df - небольшой, есть максма поворосных результатов
    - players_df - самый большой, где есть составы
    """

    headers = {
        'accept': 'application/json',
    }

    params = {
        'includeTeamMembers': '1',
        'includeMasksAndControversials': '1',
        'includeRatingB': '1'
    }

    response = requests.get('https://api.rating.chgk.net/tournaments/' + str(tourn_id) + '/results', params=params, headers=headers)
    print("Status code: ", response.status_code)

    # вот в эти списки сохраняем данные по каждому объекту
    json_data = []
    mask_data = []
    players_data = []

    i = 0
    for fixt in response.json():
        # print(i)
        venue = ''
        is_sinh = 0
        # признака синхрона как такого в данных нет, но признак площадки только у них
        try:
            venue = fixt['synchRequest']['venue']['name']
            is_sinh = 1
        except:
            venue = ''
            is_sinh = 0
        try:
            json_data.append([
                                tourn_id,
                                fixt['team']['id'],
                                fixt['team']['name'],
                                # место команды в турнире
                                fixt['position'],
                                # число взятых вопросов
                                fixt['questionsTotal'],
                                is_sinh,
                                venue,
                                fixt['rating']['inRating'],
                                fixt['rating']['rg'],
                                # бонус за турнир
                                fixt['rating']['d'],
                                fixt['rating']['predictedPosition'],
                                ])
            mask_data.append([
                            tourn_id,
                            fixt['team']['id'],
                            # строка вида 10X? где каждый символ означает вопрос, взятый, невзятый, снятый или непонятный
                            fixt['mask']
                            ])
            for player in fixt['teamMembers']:
                players_data.append([
                                tourn_id,
                                fixt['team']['id'],
                                player['player']['id'],
                                player['player']['surname'],
                                player['player']['name'],
                                # капитан, игрок базы или легионер
                                player['flag'],
                                # рейтинг игрока
                                player['rating'],
                                ])
        except:
            pass
        i = i + 1
        
    torun_df = pd.DataFrame(json_data)
    torun_df.columns = [
        'tourn_id', 'team_id', 'team_name', 'position', 'result',
        'is_sinh', 'venue', 
        'is_rating', 'rg', 'd', 'predictedPosition'
                        ]
    

    question_df = pd.DataFrame(mask_data)
    question_df.columns = ['tourn_id', 'team_id', 'mask']

    players_df = pd.DataFrame(players_data)
    players_df.columns = [
        'tourn_id', 'team_id', 'player_id', 'player_surname', 'player_name',
        'flag', 'rating', 
                        ]

    return torun_df, question_df, players_df

def get_tourn_list(date_start: str, date_end: str, page: int):
    """
    Получаем по API список турниров и всякую справочную информацию по нему
    Можно забрать больше данных, если будет желание, читайте документацию
    
    Параметры:
    date_start - строка вида 'yyyy-mm-dd', передаём в поле dateStart[after]
    date_end - строка вида 'yyyy-mm-dd', передаём в поле dateEnd[before]
    page - номер страницы пагинации (большее 500 турниров за раз не отдают)

    Возвращаем датафрейм со списком туриров
    """
    headers = {
        'accept': 'application/json',
    }

    params = {
        'dateStart[after]': date_start,
        'dateEnd[before]': date_end,
        'itemsPerPage': 500,
        'page': page
    }

    response = requests.get('https://api.rating.chgk.net/tournaments/', params=params, headers=headers)
    print("Status code: ", response.status_code)

    if response.status_code != 200:
        print("Error message: ", response.text)

    json_data = []
    for tdata in response.json():
        diff = 0
        tdl = 0
        # для старых турниров иногда его нет
        try:
            diff = tdata['difficultyForecast']
        except:
            diff = 0
        try:
            tdl = tdata['trueDL']
        except:
            tdl = 0
        
        json_data.append([
                            tdata['id'],
                            tdata['name'],
                            # очный, синхрон, онлайн или всякие асинхроны
                            tdata['type']['name'],
                            tdata['idseason'],
                            diff,
                            tdata['maiiRating'],
                            tdl,
                            tdata['questionQty'],
                            ])
    torun_list_df = pd.DataFrame(json_data)
    torun_list_df.columns = [
                        'tourn_id',
                        'tourn_name',
                        'type',
                        'season',
                        'difficulty_forecast',
                        'is_rating',
                        'trueDL',     
                        'questionQty',
                        ]

    return torun_list_df

 
def qv_from_mask(mask:str):
    """
    Функция делает из строки с расплюсовкой массив с результатами одной конкретной команды

    """
    qv=np.array([])
    # заменяем снятый вопрос на ноль
    mask=mask.replace('X', '0')
    # что делает ? не знаю, но на что-то заменить нужно
    mask=mask.replace('?', '0')
    for ch in mask:
        qv=np.append(qv,int(ch))   
    return qv

def tourn_stat(tourn_df: pd.DataFrame)->pd.DataFrame:
    """
    Приводит данные одного турнира к максимально нормализованному виду
    1 строчка - 1 вопрос 1 команды в 1 турнире
    """
    tourn_df['numqv'] = tourn_df['mask'].str.len()
    for i in range(tourn_df['numqv'].values[0]):
        tourn_df[i+1] = tourn_df['mask'].apply(lambda x: qv_from_mask(x)[i])

    del tourn_df['mask']

    # пивот наоборот
    qv_df = tourn_df.melt(
                        id_vars=[
                                    'tourn_id', 'team_id', 'numqv'#, 'rg'
                                ], 
                        var_name='question_num', 
                        value_name='qv_result'
                        )
    # qv_df['qv_result'] = qv_df['qv_result'].astype('int')

    return qv_df


def diff_stat(qv_df: pd.DataFrame)->pd.DataFrame:
    """
    Работа со сложностью вопросов.
    Нужна нормализованная таблица с какими-то рейтингами
    """
    # получаем сложность каждого вопроса в турнире
    qv_stat = qv_df.groupby(['tourn_id', 'question_num']).agg(
            result = ('qv_result', 'sum'),
            total = ('qv_result', 'count'),
            # mean_rating = ('rg', 'mean'),
        ).reset_index()
    qv_stat['difficulty'] = qv_stat['result'] / qv_stat['total']

    return qv_stat



