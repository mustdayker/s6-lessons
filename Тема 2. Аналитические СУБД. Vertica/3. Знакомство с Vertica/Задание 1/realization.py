import vertica_python

conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2023070319',       
             'password': 'zmb2aHGYQIPkxIS',
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
}

def try_select(conn_info=conn_info):
    # И рекомендуем использовать соединение вот так
    with vertica_python.connect(**conn_info) as conn:
    
        # Select 1 — ваш код здесь; 
        querry = 'SELECT 1 as a1;'
        
        cur = conn.cursor()
        cur.execute(querry) 
        
        res = cur.fetchall()
        return res