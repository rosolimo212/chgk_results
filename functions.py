import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
plt.rcParams['figure.figsize'] = (10, 8)

from multiprocessing import Pool
import time

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 5000)
pd.set_option('display.max_colwidth', 1000)

from sklearn.cluster import KMeans

# функция возвращает данные по конкретному турниру
# там дофига всего - команды, города, рейтинги, большинство в читаемом виде
# но вот  повопросные нормально не достать
# не зависит от самописных функций
def get_tourn(tourn_id):
    # если у нас на диске уже есть этот файл, то мы его берём
    try:
        d=d.read_json('get_tourn/'+str(tourn_id)+'.json')
    except Exception: # если его нет, то тянем с сайта
        t_url='http://rating.chgk.info/api/tournaments/'+str(tourn_id)+'/list'
        try:
            d=pd.read_json(t_url)
            # волшебная строчка - вытаскивает по api в формате json данные и пишет в датафрейм
            d.to_json('get_tourn/'+str(tourn_id)+'.json')
            # и записываем в файл на будущее
        except Exception:
            d=pd.DataFrame()
            # если нам ввели что-то неправильное, вернём пустой DataFrame
    return d

# функция возвращает результат данной команды в данном турнире 
# в формате +1-1+0 (сыграла "в плюс", "в минус" или "в ноль") с точки зрения рейтингового прогноза
# зависит от get_tourn()
def get_team_result(tourn_id, team_id):
    gt=get_tourn(tourn_id)
    bns=gt[gt['idteam']==team_id]['diff_bonus'].values[0]
    return np.sign(bns)