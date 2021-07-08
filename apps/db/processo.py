from .conexao import db

def criar_tabela_processo():
    with db.engine.connect() as con:
            con.execute('''CREATE TABLE IF NOT EXISTS PROCESSO (
                                processando INTEGER,
                                id INTEGER PRIMARY KEY,
                                atual INTEGER,
                                progresso INTEGER,
                                tempo INTEGER
                            );''')
            con.execute('''INSERT OR REPLACE INTO PROCESSO ("id") VALUES (1), (2), (3), (4), (5), (6), (7)''')