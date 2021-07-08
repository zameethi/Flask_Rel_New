import os
import zipfile
from configparser import ConfigParser
import ibm_db
import ibm_db_dbi

class gerar_arquivos():
    texto = ''
    progresso = ''
    def query(self, sql, file, origem, tipo=0):
        global progresso
        global texto
        config = ConfigParser()
        config.read('conf/pcom.ini')
        dbase = config.get("DB", "database")
        host = config.get("DB", "hostname")
        port = config.get("DB", "port")
        protocol = config.get("DB", "protocol")
        uid = config.get("DB", "uid")
        pwd = config.get("DB", "pwd")

        conexao = 'DATABASE = ' + dbase +';HOSTNAME=' + host + ';PORT=' + port + ';PROTOCOL=' + protocol + ';UID=' + uid + ';PWD=' + pwd + ';'
        print(conexao)
        ibm_db_conn = ibm_db.connect(conexao, "", "")
        conn = ibm_db_dbi.Connection(ibm_db_conn)
        lista = []
        try:
            cursor = conn.cursor()
            if tipo==0:
                f = open(sql, 'r', encoding='latin-1')
                query = " ".join(f.readlines())
            else:
                query = sql
            cursor.execute(query)
            curs = cursor.fetchall()

            for k in cursor.description:
                lista.append(k[0])

            if len(curs) >= 1:
                txt = open(f'{origem}/{file}' + str('.csv'), 'w+', encoding='UTF-8')
                txt.write((','.join(lista) + '\n').replace(',', ';'))


                for i, x in enumerate(curs):
                    progresso = (int(100.0 / float(len(curs)) * float(i)))
                    lista = []
                    for n, y in enumerate(x):
                        lista.append(str(y).replace(',','-'))
                    txt.write((','.join(lista) + '\n').replace(',', ';'))

        except Exception as e:
            print('Não foi possível executar | ', e)
            progresso = False
            texto = str(e)
        finally:
            texto = file, origem

        return texto

    def zippar(self, arqs, origem):
        rel_zip = zipfile.ZipFile(f'{origem}/{arqs}.zip', 'w')
        print(arqs)
        for folder, subfolders, files in os.walk(f'{origem}'):
            for i, file in enumerate(files):
                if file.endswith('.csv'):
                    rel_zip.write(os.path.join(folder, file), os.path.relpath(os.path.join(folder, file), f'{origem}'),
                                  compress_type=zipfile.ZIP_DEFLATED)


        rel_zip.close()
        texto = rel_zip.filename
        return texto

