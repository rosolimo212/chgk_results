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