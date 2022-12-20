import psycopg2
from pprint import pprint


def create_db(conn):
    cur = conn.cursor()
    # удаление всех таблиц
    cur.execute("""
    DROP TABLE phone;
    DROP TABLE client;
    """)
    # создание таблицы КЛИЕНТ
    cur.execute("""
    CREATE TABLE IF NOT EXISTS client(
    id SERIAL PRIMARY KEY,
    last_name VARCHAR(80),
    first_name VARCHAR(80),
    email VARCHAR(80)
    );
    """)
    # создание таблицы ТЕЛЕФОН
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phone(
    id SERIAL PRIMARY KEY,
    number NUMERIC UNIQUE,
    client_id INTEGER NOT NULL REFERENCES client(id)
    );
    """)
    conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    cur = conn.cursor()
    if phones:
        if len(phones) == 3:
            cur.execute("""
            INSERT INTO client(last_name, first_name, email) VALUES(%s, %s, %s) RETURNING id
            """, (last_name, first_name, email))
            current_id = cur.fetchone()
            cur.execute("""
            INSERT INTO phone(number, client_id) VALUES(%s, %s)
            """, (phones[0], current_id))
            cur.execute("""
            INSERT INTO phone(number, client_id) VALUES(%s, %s)            
            """, (phones[1], current_id))
            cur.execute("""
            INSERT INTO phone(number, client_id) VALUES(%s, %s)            
            """, (phones[2], current_id))
        if len(phones) == 2:
            cur.execute("""
            INSERT INTO client(last_name, first_name, email) VALUES(%s, %s, %s) RETURNING id
            """, (last_name, first_name, email))
            current_id = cur.fetchone()
            cur.execute("""
            INSERT INTO phone(number, client_id) VALUES(%s, %s)
            """, (phones[0], current_id))
            cur.execute("""
            INSERT INTO phone(number, client_id) VALUES(%s, %s)
            """, (phones[1], current_id))
        if len(phones) == 1:
            cur.execute("""
            INSERT INTO client(last_name, first_name, email) VALUES(%s, %s, %s) RETURNING id
            """, (last_name, first_name, email))
            current_id = cur.fetchone()
            cur.execute("""
            INSERT INTO phone(number, client_id) VALUES(%s, %s)
            """, (phones, current_id))
    else:
        cur.execute("""
        INSERT INTO client(last_name, first_name, email) VALUES(%s, %s, %s) RETURNING id
        """, (last_name, first_name, email))
        conn.commit()


def add_phone(conn, client_id, phone):
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO phone(number, client_id) VALUES(%s, %s)
    """, (phone, client_id))
    conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    cur = conn.cursor()
    if first_name:
        cur.execute("""
        UPDATE client 
        SET first_name=%s 
        WHERE id=%s;
        """, (first_name, client_id))
    if last_name:
        cur.execute("""
        UPDATE client 
        SET last_name=%s 
        WHERE id=%s;
        """, (last_name, client_id))
    if email:
        cur.execute("""
        UPDATE client 
        SET email=%s 
        WHERE id=%s;
        """, (email, client_id))
    if phones:
        cur.execute("""
        SELECT id FROM phone 
        WHERE number=%s; 
        """, (phones[0],))
        id = cur.fetchone()
        cur.execute("""
        UPDATE phone
        SET number=%s
        WHERE id=%s;
        """, (phones[1], id))
    conn.commit()


def delete_phone(conn, phone):
    cur = conn.cursor()
    cur.execute("""
    DELETE FROM phone
    WHERE(number=%s)
    """, (phone,))
    conn.commit()


def delete_client(conn, client_id):
    try:
        cur = conn.cursor()
        cur.execute("""
        DELETE FROM client WHERE(id=%s);
        """, (client_id,))
        conn.commit()
    except:
        print('Удалите сначала телефоны этого пользователя.')
        exit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    cur = conn.cursor()
    if first_name:
        cur.execute('''
        SELECT * FROM client AS c
        LEFT JOIN phone AS p ON p.client_id = c.id
        WHERE(first_name=%s)
        GROUP BY c.id, p.id;
        ''', (first_name,))
        pprint(cur.fetchall())
    if last_name:
        cur.execute('''
        SELECT * FROM client AS c
        LEFT JOIN phone AS p ON p.client_id = c.id
        WHERE(last_name=%s)
        GROUP BY c.id, p.id;
        ''', (last_name,))
        pprint(cur.fetchall())
    if email:
        cur.execute('''
        SELECT * FROM client AS c
        LEFT JOIN phone AS p ON p.client_id = c.id
        WHERE(email=%s)
        GROUP BY c.id, p.id;
        ''', (email,))
        pprint(cur.fetchall())
    if phone:
        cur.execute('''
        SELECT * FROM client AS c
        LEFT JOIN phone AS p ON p.client_id = c.id
        WHERE(number=%s)
        GROUP BY c.id, p.id;
        ''', (phone,))
        pprint(cur.fetchall())
    else:
        print('There is no such client.')


def show_db_client(conn):
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM client
    """)
    pprint(cur.fetchall())


def show_db_phone(conn):
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM phone
    """)
    pprint(cur.fetchall())


def show_all_in_one(conn):
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM client AS c
    LEFT JOIN phone AS p ON p.client_id = c.id
    GROUP BY c.id, p.id
    """)
    pprint(cur.fetchall())


with psycopg2.connect(database='psql_hw', user='postgres', password='MPuzo1920') as conn:
    create_db(conn) # ГОДНО
    # add_client(conn, first_name='stas', last_name='golovin', email='golovin.ds93@gmail.com') # ГОДНО
    add_client(conn, first_name='igor', last_name='vasilchenko', email='horugvi@mail.ru', phones=('79991284562',)) # ГОДНО
    # add_client(conn, first_name='pasha', last_name='maksimov', email='maksimov.ds93@gmail.com', phones=('79524245687', '9950864531')) # ГОДНО
    # add_phone(conn, client_id=1, phone='79530864531') # ГОДНО
    # add_phone(conn, client_id=1, phone='79184170353') # ГОДНО
    # add_phone(conn, client_id=2, phone='2012048') # ГОДНО
    # change_client(conn, client_id=1, first_name='Alex', last_name='Pushkin') #ГОДНО
    # change_client(conn, client_id=1, first_name='petya', phones=('79530864531', '88002353535')) #ГОДНО
    # change_client(conn, client_id=1, phones=('79184170353', '7993456781')) #ГОДНО
    # delete_phone(conn, phone='79530864531') # ГОДНО
    # delete_client(conn, client_id=2) # ГОДНО
    # find_client(conn, first_name=None, last_name=None, email=None, phone=None) # ГОДНО
    # find_client(conn, first_name='stas') # ГОДНО
    # find_client(conn, last_name='maksimov') # ГОДНО
    # find_client(conn, email='golovin.ds93@gmail.com') # ГОДНО
    # find_client(conn, phone='79530864531') # ГОДНО
    # show_db_client(conn)
    # show_db_phone(conn)
    show_all_in_one(conn)
conn.close()
