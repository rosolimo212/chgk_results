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

def get_tourn_result(tourn_id):
    gt=get_tourn(tourn_id)
    gt=gt[['idteam', 'current_name', 'diff_bonus']]
    gt['tourn_id']=tourn_id
    gt['result']=np.sign(gt['diff_bonus'])
    gt.columns=['team_id', 'name', 'diff_bonus', 'tourn_id', 'result']
    return gt

# вытаскивае расплюсовку команды в данном турнире в ненормализованном виде
def get_team(tourn_id, team_id):
    try:
        d=d.read_json('get_team_from_tourn/'+str(tourn_id)+'-'+str(team_id)+'.json')
        # то же самое: сначала тащим с диска, потом из сети
    except Exception:
        try:  # обработка исключений на случай падения интернета, 404 и тп
            d=pd.read_json('http://rating.chgk.info/api/tournaments/'+str(tourn_id)+'/results/'+str(team_id))
            # волшебная строчка - вытаскивает по api в формате json данные и пишет в датафрейм
            # адреса других вошебных строчек: http://rating.chgk.info/index.php/api
            d.to_json('get_team_from_tourn/'+str(tourn_id)+'-'+str(team_id)+'.json')
            return d
        except Exception: 
            d=pd.DataFrame()
            return d    
        
# функция, которая возвращает DatFrame строку: повопросные резульататы команды team_id в турнире tourn_id
def get_team_from_tourn(tourn_id, team_id):
    d=get_team(tourn_id, team_id)
    
    # лично меня бесит разбивка повопросных результатов по турам и ненормализованный вид таблиц из-за этого
    # в связи с этим начинаю некоторые танцы с бубном
    
    num_t=max(d['tour'])     # Зафиксировали число туров 
    num_qv=len(d['mask'][0]) # зафиксировали число вопросов в туре
    
    # TO DO: проверить, что бывает с турнирами, где в турах разное число вопросов
    
    rplus=list(d['mask'])
    # mask - это поле, в котором лежит список с ответами команды в туре 
    # в формате 1 (взяытй), 0 (не взятый), X - снятый (зачем?)
    # формат не очень удобный, но в экспоте с турниром вообще не распаршивается
    
    
    # ниже вытаскиваем из mask данные каждого вопроса и создаём для него свой столбец в DataFrame
    tt=0 # Счётчик вопроса, начинаем с 1
    s_l=[]  # Список с заголовком столбца
    r_l=[]  # Список со значением столбца
    
    for j in range(num_t):
        for i in range(num_qv):
            tt=tt+1
            r_l.append(rplus[j][i])
            s_l.append('qv'+str(tt))
    
    # Через словарь записываем всё в DataFrame. Наверняка можно сделаь проще
    res={} 
    res = {'tourn_id': tourn_id, 'team_id': team_id} # поля с парамерами команды и турнира появляются в пустом словаре
    for i in range(len(s_l)):
        res.update({s_l[i]:r_l[i]})   # в цикле добавляем по одной записи на каждый вопрос, это не должно быть долго
        
    d=pd.DataFrame([res], columns=res.keys())
    d=d.replace('X', 0)  # X - это снятый вопрос
    d=d.astype('Int64')
    return d


# функция выводит расплюсовку всех команд турнира
# функция дублирует поле mask в параметрах турнира, но его нельзя корректно загрузить в pandas
# для работы нужны get_tourn() и get_team_from_tourn()
def get_tourn_plus(tourn_id):
    # Достаём список команд из одного API, а расплюсовку - из другого, в цикле по командам, потом клеим
    try:
        r=pd.read_csv('get_tourn_plus/'+str(tourn_id)+'.csv')
        # то же самое: сначала тащим с диска, потом из сети
        
        del r['Unnamed: 0']
        return r 
    except Exception:
        teams_list=list(get_tourn(tourn_id)['idteam']) # всё, что нам сейчас нужно - список команд

        r=pd.DataFrame()
        for i in range(len(teams_list)):
            b=get_team_from_tourn(tourn_id, teams_list[i])
            # описание этой функции выше
            r=pd.concat([r,b])
        r=r.astype('Int64') # иначе будет строка и когда начнём сумму считать будет сюрприз
        r=r.replace('X', 0) # X - это снятый вопрос
        #r.to_json('get_tourn_plus/'+str(tourn_id)+'.json')
        r.to_csv('get_tourn_plus/'+str(tourn_id)+'.csv')
        return r
# TO DO: откуда тут строка с нулями? - от хренового экспорта в csv. как фиксить - непонятно



# функция вывод поповпросные резульатты лучших top команд выбранного турнира
# для работы нужна get_tourn_plus()
def get_team_top(tourn_id, top):
    # подкинем результат для лучших н команд
    
    df=get_tourn_plus(tourn_id)
    
    tp=df  # временный даатфрейм для извращений

    trb=tp['tourn_id']
    tmd=tp['team_id']  # сначала сохраняем в буфер стобцы с командами и турниром

    del tp['tourn_id'] # потом убиваем их
    del tp['team_id']

    tp['sum']=tp.sum(axis=1)  # вот можно посчитать сумму по строке, вы знали?

    tp['tourn_id']=trb  # возвращаем блудные idшники
    tp['team_id']=tmd

    tp=tp.sort_values(by='sum',  ascending=False) # сортиуем по взятым
    
    return tp[0:top] # берём только топ


# функция для расчёта рейтинга и сложности вопросов
# кроме того, функция пытается разбить вопросы на классы сложности
# для работы нужны get_tourn_plus() и пакет для работы kmeans
def tourn_dif(tourn_id):
    # рейтинг взятых вопросов здесь и ниже считается немного необычно: как доля невзятых вопросов
    # обычно на турнирах нормировкой рейтинга не занимаются, 
    # но на нужно будет сравнивать сложность вопросов разных турниров
    
    df=get_tourn_plus(tourn_id) # вытащили расплюсовку конкретного турнира
    # описание этой функции выше

    df=df.replace('X', 0)  # ставим нолики вместо снятых
    
    g1 = df.groupby(['tourn_id']).agg('sum')
    # группировка по числу взятых
    g2 = df.groupby(['tourn_id']).agg('count')
    # группировка по общей сумме, заложились на случай туринров по системе микроматчей
    
    g=pd.concat([g1,g2])
    # да-да, скажите мне, что это неизящно
    
    g=g.reset_index()
    # возможный источник проблем
    
    
    
    # ниже будут совсем неочевидные решения, возможно, тут надо всё переделать
    gtmp=g
    del gtmp['tourn_id']
    del gtmp['team_id']
    # после удаления id в датафрейме только вопросы и показатели числа взятых
    
    v=g.transpose()
    
    v.columns = ['sum', 'total']
    v['share']=1-(v['sum']/v['total'])  # это и есть сложность, чем больше, тем вопрос сложнее
    
    v['qv_num']=range(1,len(v['share'])+1)
    # номер вопроса отдельным понял понадобился для будущей сортировке и из-за транспонирования
    #v['share'].plot() график вполне познавательный
    
    ###
    # возможно, тут функцию стоит закончить, мы получили нужные нам метрики
    ###
    
    v=v.sort_values(by='share', ascending=True)
    # сортировка в порядке убывания сложности
    # по идее она не должна влиять на результат кластеризации, хотя кто его знает
    
    # дальше мы с помощью стандартного K-means пытаемся разбить вопросы на классы сложности
    X=v[['total', 'share']].values  # бахнули число вопросов, так как стандартный интерфейс K-means двумерный
    kmeans = KMeans(n_clusters=4, random_state=0).fit(X)  # 4 кластера - не догма, но выглядит разумно
    
    v['label']=(kmeans.labels_)
    v['class']=(kmeans.labels_)/max(kmeans.labels_) 
    # нормировка лейблов: 
    # во-первых, графики на одной оси,
    # во-вторых в случае изменения числа кластеров у старшего всё равно будет 1 
    
    #v[['class', 'share']].plot()
    # важный график
    v=v.replace('X', 0)
    
    # считаем среднюю сложность по группам вопросов
    g=pd.DataFrame(v.groupby(['label', 'class'])['share'].mean())
    g=g.reset_index()
    g.columns=['label', 'class', 'class_chare']
    
    v=v.merge(g, left_on=['class', 'label'], right_on=['class', 'label'], how='outer')
    
    v=v.sort_values(by='qv_num', ascending=True)
    # над правильной сортировкой надо подумать
    
    return v


# функция, которая возвращает рейтиг команды на турнире
# для работы нужны get_team_from_tourn() и =tourn_dif()
def get_team_rating(tourn_id, team_id):
    # Рейтинг команды - это сумма рейтинга вопросов, которые она взяла
    # Тянем рейтинги вопросов, тянет повопросник команды, конвертируем в списки и сумма произведений
    
    # пока не работает корректно
    
    tm=get_team_from_tourn(tourn_id, team_id)
    # тянем повопросные команды, описание этой функции выше
    
    tm=tm.astype('Int64')
    
    del tm['tourn_id']
    del tm['team_id']
    # теперь ничего лишнего
    
    t_p=list(tm.values[0])
    # конвертируем в список
    
    trn=tourn_dif(tourn_id)
    # тянем сложность вопросов турниров, описание этой функции выше
    
    trn=trn.sort_values(by='qv_num')
    # тут сортировка по номеру вопроса очень важна
    
    trn=trn.transpose()
    sh=list(trn.loc['share'].astype('float'))
    # конвертируем в список
    
    # стандартная вещь из NumPy почему-то не хочет работать с float
    # ну и чёрт с ним, пишем сумму произведений вручную, это не должно быть долго
    s=0
    for i in range(len(t_p)):
        s=s+sh[i]*t_p[i]
    return s

# вспомогательная функция, выводит результат топа команд по категориям сложности
# для работы нужны get_team_top()
def get_top_dif(tourn_id):
    top=20
    tm=get_team_top(tourn_id, top)
    if len(tm)<20: 
        top=3
        tm=get_team_top(tourn_id, top)


    del tm['tourn_id']
    del tm['team_id']  # режем ненужное
    del tm['sum']

    tm=tm.transpose()

    st=[]
    qv_n=[]
    for i in range (1,top+1):
        st.append('t_'+str(i))

    tm.columns=st
    tm['t_dif']=tm.sum(axis=1)/top


    tm['qv_num']=range(1,len(tm['t_dif'])+1)
    tm=tm[['qv_num', 't_dif']]


    td=tourn_dif(tourn_id)
    td=td.merge(tm, left_on=['qv_num'], right_on=['qv_num'], how='outer')

    
    return td

# функция для определенимя результата команды в зависимости от категорий сложности вопросов
# для работы нужнры get_team_from_tourn(), tourn_dif()
def team_tourn_cat_dif(tourn_id, team_id):
    tm=get_team_from_tourn(tourn_id, team_id)
    # загрузили результат команды
    
    del tm['tourn_id']
    del tm['team_id']  # режем ненужное
    tm=tm.transpose()
    
    df=get_top_dif(tourn_id)

    # берём турнир
    df['team']=tm.values  # теперь в датафрейме турнира есть стобец команды
    df['team']=df['team'].astype('Int64')
    
    g1=pd.DataFrame(df.groupby(['class'])['team'].sum())
    g2=pd.DataFrame(df.groupby(['class'])['team'].agg('count'))
    g3=pd.DataFrame(df.groupby(['class'])['share'].agg('mean'))
    g4=pd.DataFrame(df.groupby(['class'])['t_dif'].agg('mean'))
    # сгруппировали интеерсующие нас метрики
    
    g=pd.concat([g1,g2, g3, g4], axis=1)
    g=g.reset_index()
    
    g.columns=['class', 'plus', 'total', 'dif', 'top']
    g['team_share']=g['plus']/g['total']
    # посчитали долю взятых на классе
    
    g['avg_share']=1-g['dif']
    
    g=g.sort_values(by='dif')
    
    g['mark']=np.where(g['team_share'] >=g['top'], 2, np.where(g['team_share'] >=g['avg_share'], 1, 0))
    
    #g=g.reset_index()
    
    # осортировали классы по сложности турнира
    return g

def show_tourn_dist(tourn_id):
    v=tourn_dif(tourn_id)
    
    v=v.sort_values(by='qv_num', ascending=True)
    
    with plt.xkcd():
        plt.title('Tourn '+str(tourn_id))
        plt.xlabel('Number_of_qv')
        plt.ylabel('Qv_difficulty')
        plt.plot(v['qv_num'], v['share'])

        #v=v.sort_values(by='share', ascending=True)

        #v[['class', 'share']].plot(kind='area')

def team_mark(tourn_id, team_id):
    df=team_tourn_cat_dif(tourn_id, team_id)
    df=df.sort_values(by='dif')
    df=df.set_index('dif')
    if df['plus'].sum()<6:
        res='weak'
    elif ((df['mark'].values[0]+df['mark'].values[1])) > ((df['mark'].values[2]+df['mark'].values[3])):
        res='tech'
    elif ((df['mark'].values[0]+df['mark'].values[1])) < ((df['mark'].values[2]+df['mark'].values[3])):
        res='creat'
    elif (df['mark'].values[0]==df['mark'].values[1]) and (df['mark'].values[1]==df['mark'].values[2]) and (df['mark'].values[2]==df['mark'].values[3]):
        res='stab'
    else:
        res='unst'
    return res

def tourn_mark(tourn_id):
    try:
        df=pd.read_csv('tourn_mark/'+str(tourn_id)+'.csv')
    except Exception: 
        t_l=[]
        teams_list=list(get_tourn(tourn_id)['idteam'])
        teams_names=list(get_tourn(tourn_id)['current_name'])

        i=0
        for i in range(len(teams_list)):
            t_l.append(team_mark(tourn_id, teams_list[i]))
        df=pd.DataFrame(t_l)

        df.columns=['type']
        df['team_id']=1
        df['team_id']=teams_list
        df['current_name']=teams_names
        df.to_csv('tourn_mark/'+str(tourn_id)+'.csv')
    return df

def show_tourn_lev(tourn_id):
    v=tourn_dif(tourn_id)
    with plt.xkcd():
    
        v=v.sort_values(by='share', ascending=True)
        p=v[['qv_num', 'class', 'share']]
        plt.xlabel('Questions')
        plt.ylabel('difficulty')

        plt.plot(range(len(p['share'])), p['share'])

        
        plt.show()
        
def show_tourn_levl(tourn):
    v=[]
    for i in range(len(tourn)):
        v.append(tourn_dif(tourn[i]))
        
    for i in range(len(tourn)):
        with plt.xkcd():
            v[i]=v[i].sort_values(by='share', ascending=True)
            plt.xlabel('Questions')
            plt.ylabel('difficulty')
            

            plt.plot(range(len(v[i]['share'])), v[i]['share'], label=tourn[i])

    plt.legend()
    plt.show()

    
def plmin(tourn):
    res=pd.DataFrame()
    for tourn_id in tourn:
        df=get_tourn_result(tourn_id)
        res=pd.concat([res, df])
    g=res.groupby('result').agg({'team_id': lambda x: x.nunique()})
    g=g.reset_index()
    g['share']=g['team_id']/sum(g['team_id'])
        
    return str(round(100*g[g['result']==1]['share'].values[0],1))+'%'


def team_cat_res(tourn):
    rt=pd.DataFrame()
    mt=pd.DataFrame()
    for tourn_id in tourn:
        try:
            r=get_tourn_result(tourn_id)
            m=tourn_mark(tourn_id)
            rt=pd.concat([rt, r])
            mt=pd.concat([mt, m])
            print(tourn_id)
        except Exception:
            pass
    mrg=rt.merge(mt, 'left', on='team_id')

    g=mrg.groupby(['type', 'result']).agg({'team_id': lambda x: x.nunique()})
    g=g.reset_index()

    gg=g.groupby('type').sum()
    gg=gg.reset_index()
    gg.columns=['type', 'r', 'sum']

    g=g.merge(gg, 'left', on='type')
    g['share']=round(100*g['team_id']/g['sum'], 1)
    g=g[g['result']==1][['type', 'team_id', 'share']]
    g['share']=g['share'].astype('str')
    g['share']=g['share']+'%'
    g['avg']=plmin(tourn)
    g['res']=np.where(g['share']>g['avg'],'better',np.where(g['share']<g['avg'],'worse',np.where(g['share']==g['avg'],'same','error')))
    return g

def parse_tourn(start, stop):
    l=[]
    for i in range(start, stop):
        try:
            a=get_tourn(i)['diff_bonus'][0]
            l.append(i)
        except Exception:
            pass
    return l

def team_stat(tourn, team_id):
    try:
        df_r=pd.read_csv('team_stat/'+str(tourn_id)+'-'+str(team_id)+'.scv')
    except Exception:
        df_r=pd.DataFrame()
        for tourn_id in tourn:
            df=get_tourn(tourn_id)
            df=df[['idteam', 'questions_total', 'base_name', 'diff_bonus']]
            df['tourn_id']=tourn_id
            df.columns=['team_id', 'qst', 'team_name', 'result', 'tourn_id']
            df=df[df['team_id']==team_id]
            df_r=pd.concat([df_r, df])
            df.to_csv('team_stat/'+str(tourn_id)+'-'+str(team_id)+'.scv')
    return df_r

def teams_stat(tourn, team1, team2):
    t1=team_stat(tourn, team1)
    t2=team_stat(tourn, team2)
    t=t1.merge(t2, 'outer', on='tourn_id', suffixes=['_1', '_2'])
    t=t.fillna(-1)
    t['score']=np.where(t['qst_1']>t['qst_2'],1.0,np.where(t['qst_1']==t['qst_2'],0.5,0))
    t['score']=np.where(t['qst_1']==-1, 0, t['score'])
    t['score']=np.where(t['qst_2']==-1, 0, t['score'])
    return t

def team_score(tourn, team1, team2):
    t1=teams_stat(tourn, team1, team2)
    t2=teams_stat(tourn, team2, team1)
    s1=sum(t1['score'])
    s2=sum(t2['score'])
    n=t1[(t1['qst_1']>0)&(t1['qst_2']>0)]
    g=len(n)
    print('Score: '+str(s1)+' - '+str(s2)+' in '+str(g)+' games')

def get_tourn_result(tourn_id):
    gt=get_tourn(tourn_id)
    gt=gt[['idteam', 'current_name', 'diff_bonus']]
    gt['tourn_id']=tourn_id
    gt['result']=np.sign(gt['diff_bonus'])
    gt.columns=['team_id', 'name', 'diff_bonus', 'tourn_id', 'result']
    return gt

























        

        
       