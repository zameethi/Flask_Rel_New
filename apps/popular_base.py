import timeit
import os
import pandas as pd
import sqlite3 as db
print(os.path.dirname(__file__)+'/db')
def popularbase():
    path = os.path.dirname(__file__)+'/db'
    start = timeit.default_timer()
    print('populando!!', path)
    file1 = f'{path}/csv/QUERY SORTIMENTO SEÇÃO 1.csv'
    file2 = f'{path}/csv/QUERY SORTIMENTO SEÇÃO 2.csv'
    file3 = f'{path}/csv/QUERY SORTIMENTO SEÇÃO TELEFONIA.csv'
    file4 = f'{path}/csv/QUERY SORTIMENTO SEÇÃO MOVEIS 1.csv'
    file5 = f'{path}/csv/QUERY SORTIMENTO SEÇÃO MOVEIS 2.csv'
    file6 = f'{path}/csv/QUERY SORTIMENTO - SORTIM.csv'

    names = ['CODSORTIMENTO', 'CODSECAO', 'DESCSECAO', 'CODTAMANHO', 'DESCTAMANHO', 'CODPUBLICO',
                            'DESCPUBLICO', 'CODMERCADORIA', 'DESCMERCADORIA', 'MERCCONJ', 'PRIORIDADE', 'QTDE', 'VOLTAGEM',
                            'VIGENCIAINICIO', 'VIGENCIAFIM', 'STATUS', 'SETOR', 'DESCSETOR', 'CLASSE', 'DESCCLASSE',
                            'ESPECIE', 'DESCESPECIE', 'MARCA', 'DESCMARCA']
    try:
        con = db.connect(f'{path}/sortimento.db')
        cursor = con.cursor()
    except Exception as e:
        print(e)
    try:
        cursor = con.cursor()
        cursor.execute('delete from sortemp;')
        cursor.execute('delete from sortim;')
        con.commit()
        
    except Exception as e:
        print(e)

    try:
        df1 = pd.read_csv(file1, low_memory=False, header=0, encoding='utf-8', sep=';',
                        names=names)
    except Exception as e:
        print(e)
    print('leu 1')

    df2 = pd.read_csv(file2, low_memory=False, header=0, encoding='utf-8', sep=';',
                        names=names)
    print('leu 2')
    df3 = pd.read_csv(file3, low_memory=False, header=0, encoding='utf-8', sep=';',
                        names=names)
    print('leu 3')
    df4 = pd.read_csv(file4, low_memory=False, header=0, encoding='utf-8', sep=';',
                        names=names)
    print('leu 4')
    df5 = pd.read_csv(file5, low_memory=False, header=0, encoding='utf-8', sep=';',
                        names=names)
    print('leu 5')
    df6 = pd.read_csv(file6, low_memory=False, header=0, encoding='utf-8', sep=';',
                        names=['EMPRESA','FILIAL','VOLTAGEM','CODPUBLICO','DESCPUBLICO','CODSECAO','DESCSECAO','CODSORTIMENTO','CODTAMANHO','DESCTAMANHO','TIPOPUBLICO','SOLICVOLTDIF'])
    print('leu 6')
    df = df1.append([df2, df3, df4, df5], ignore_index=True)
    print('leu uniou')

    df.DESCESPECIE = df.DESCESPECIE.apply(lambda x: 'NÃO ENCONTRADO' if 'NÃ' in x else x)
    df.DESCSETOR = df.DESCSETOR.apply(lambda x: 'NÃO ENCONTRADO' if 'NÃ' in x else x)
    df.DESCCLASSE = df.DESCCLASSE.apply(lambda x: 'NÃO ENCONTRADO' if 'NÃ' in x else x)
    df.DESCMARCA = df.DESCMARCA.apply(lambda x: 'NÃO ENCONTRADO' if 'NÃ' in x else x)
    df.DESCMERCADORIA = df.DESCMERCADORIA.str.replace('\s+', ' ')
    df = df[df.DESCMERCADORIA != 'None']
    df = df[df.VOLTAGEM != 'None']
    
    
    print('mandando para sortemp e sortim')
    df.to_sql(name='SORTEMP', con=con, if_exists='replace', chunksize=100000)
    print('1')
    
    
    print('sortemp na base!')
    df6.to_sql(name='SORTIM', con=con, if_exists='replace', chunksize=100000)
    print('sortim na base!')
    print('terminou!')
    stop = timeit.default_timer()
    

    print('executado!!!')
    print(f'Time:, {(stop - start) / 60}')
    return str('ok')
