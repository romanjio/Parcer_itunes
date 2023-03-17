''' 
Itunes parcer
Эта программа предназначена для парсинга файла iTunes Library.xml и записи данных о треках в базу данных SQLite.
'''


import xml.etree.ElementTree as ET
import sqlite3
import os
from tabulate import tabulate

#используем менеджер контекста для открытия и закрытия соединения с базой данных
with sqlite3.connect('music.sqlite') as conn: 
    cur = conn.cursor()
    
    cur.executescript('''
    DROP TABLE IF EXISTS Artist;
    DROP TABLE IF EXISTS Album;
    DROP TABLE IF EXISTS Track;
    DROP TABLE IF EXISTS Genre;
    
    CREATE TABLE Artist (
        id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name    TEXT UNIQUE
    );
    
    CREATE TABLE Genre (
        id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        name    TEXT UNIQUE
    );
    
    CREATE TABLE Album (
        id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        artist_id  INTEGER,
        title   TEXT UNIQUE
    );
    
    CREATE TABLE Track (
        id  INTEGER NOT NULL PRIMARY KEY 
            AUTOINCREMENT UNIQUE,
        title TEXT  UNIQUE,
        album_id  INTEGER,
        genre_id  INTEGER,
        len INTEGER, rating INTEGER, count INTEGER
    );
    ''')
    conn.commit()                
        
# вводим название файла   
fname = input('Enter file name: ')  

# Если введено пустое значение или файл не существует
if not fname or not os.path.isfile(fname):
    print(f"File {fname} does not exist")
    fname = 'Library.xml'
    #fname = 'Медиатека.xml'
# Определяется функция lookup(), которая принимает на вход xml-дерево и ключ для поиска   
def lookup(d, key):
    found = False
    for child in d:
        if found : return child.text
        if child.tag == 'key' and child.text == key :
            found = True
    return None

# Считывается содержимое файла и ищется все элементы типа 'dict'
stuff = ET.parse(fname)
all = stuff.findall('dict/dict/dict')
print('Dict count:', len(all))    

    
with sqlite3.connect('music.sqlite') as conn: 
    cur = conn.cursor()    
    for entry in all:
        if ( lookup(entry, 'Track ID') is None ) : continue
    
        # используя функцию lookup получаем значения полей
        name = lookup(entry, 'Name')
        artist = lookup(entry, 'Artist')
        album = lookup(entry, 'Album')
        genre = lookup(entry, 'Genre')
        count = lookup(entry, 'Play Count')
        rating = lookup(entry, 'Rating')
        length = lookup(entry, 'Total Time')
        
        # Если какое-либо из этих значений пустое, пропускаем текущую итерацию цикла
        if name is None or artist is None or album is None or genre is None: 
            continue

        #print(name, artist, album,  genre, count, rating, length)
        
        cur.execute('''INSERT OR IGNORE INTO Artist (name) 
            VALUES ( ? )''', ( artist, ) )
            
        # Выполняем запрос к таблице Artist для получения идентификатора исполнителя   
        cur.execute('SELECT id FROM Artist WHERE name = ? ', (artist, ))
        # сохраняем его в переменной artist_id
        artist_id = cur.fetchone()[0]

        cur.execute('''INSERT OR IGNORE INTO Album (title, artist_id) 
            VALUES ( ?, ? )''', ( album, artist_id ) )
        cur.execute('SELECT id FROM Album WHERE title = ? ', (album, ))
        album_id = cur.fetchone()[0]
        
        cur.execute('''INSERT OR IGNORE INTO Genre (name) 
            VALUES ( ?)''', ( genre, ) )
        cur.execute('SELECT id FROM Genre WHERE name = ? ', (genre, ))
        genre_id = cur.fetchone()[0]
        
        cur.execute('''INSERT OR REPLACE INTO Track
            (title, album_id, genre_id, len, rating, count) 
            VALUES ( ?, ?, ?, ?, ?, ?)''', 
            ( name, album_id, genre_id, length, rating, count ) )

        conn.commit() 
               
with sqlite3.connect('music.sqlite') as conn: 
    cur = conn.cursor()          
    cur.execute('''
    SELECT Track.title, Artist.name, Album.title, Genre.name 
    FROM Track JOIN Genre JOIN Album JOIN Artist 
    ON Track.genre_id = Genre.ID and Track.album_id = Album.id 
        AND Album.artist_id = Artist.id
    ORDER BY Track.title LIMIT 10
    ''' )
    rows = cur.fetchall()
    headers = ['Название песни', 'Имя исполнителя', 'Название альбома', 'Жанр']
    print(tabulate(rows, headers=headers))  
    

        
