import glob
import multiprocessing
import os
import sqlite3
import timeit
from datetime import datetime
from pathlib import Path
from shutil import copy

import apscheduler.schedulers.background
import requests
from flask import redirect, render_template, send_file, url_for
from flask_wtf import FlaskForm
from wtforms import SelectMultipleField
from wtforms.validators import DataRequired

from apps import (criar_campo_minado)
from apps.secoes import *
from vars_pandas import *
from apps.db.conexao import app, db, port
from apps.popular_base import popularbase
from choices_form import choices as ch
from cruzada_sql import sql
from processos.gerar_relatorio import gerar_arquivos
from apps.db.processo import criar_tabela_processo

scheduler = apscheduler.schedulers.background.BackgroundScheduler()

b = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar', daemon=True, args=(1, 'concatenado', '1', 'tabela_perfil_tamanho', 'ordem_PT', 'Perfil_tamanho'))
c = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar1', daemon=True, args=(2, 'concatenado', '1', 'tabela_perfil_tamanho', 'ordem_PT', 'Perfil_tamanho'))
d = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar2', daemon=True, args=(3, 'concatenado', '1', 'tabela_perfil_tamanho', 'ordem_PT', 'Perfil_tamanho'))
e = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar3', daemon=True, args=(4, 'concatenado', '1', 'tabela_perfil_tamanho', 'ordem_PT', 'Perfil_tamanho'))
f = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar4', daemon=True, args=(5, 'concatenado', '1', 'tabela_perfil_tamanho', 'ordem_PT', 'Perfil_tamanho'))
g = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar5', daemon=True, args=(6, 'concatenado', '1', 'tabela_perfil_tamanho', 'ordem_PT', 'Perfil_tamanho'))
h = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar6', daemon=True, args=(7, 'concatenado', '1', 'tabela_perfil_tamanho', 'ordem_PT', 'Perfil_tamanho'))


def first_run():
    criar_tabela_processo()
    with db.engine.connect() as con:
        con.execute("UPDATE PROCESSO SET processando = 0 where id in (1,2,3,4,5,6,7);")
        con.execute("UPDATE PROCESSO SET atual = '' where id in (1,2,3,4,5,6,7);")
        con.execute("UPDATE PROCESSO SET progresso = '100' where id in (1,2,3,4,5,6,7);")
        con.execute("UPDATE PROCESSO SET tempo = '0' where id in (1,2,3,4,5,6,7);")
    try:
        b.terminate()
        c.terminate()
        d.terminate()
        e.terminate()
        f.terminate()
        g.terminate()
        h.terminate()
    except:
        pass

class Secoes_form(FlaskForm):
    secao = SelectMultipleField('secao', [DataRequired()],
                                choices=ch)


def resultado():
    file = gerar_arquivos()
    for root, _, files in os.walk("processos/sql_sortim_sortemp", topdown=False):
        for name in files:
            result = file.query(os.path.abspath(os.path.join(root, name)), Path(name).stem,
                                os.path.relpath('media/sortimento'))
        for filename in glob.glob(os.path.join(os.path.abspath('media/sortimento'), '*.csv')):
            copy(filename, os.path.dirname(__file__)+'/apps/db/csv')
    names = file.zippar(f'SortimSortemp-{str(datetime.now()).replace(":", "_")[:-10]}',
                        os.path.relpath('media/sortimento'))
    print(names)
	
    try:
        b.terminate()
        c.terminate()
        e.terminate()
        f.terminate()
        g.terminate()
        h.terminate()
    except:
        pass
    t = popularbase()
    print(t, 'popular base')
    try:
        os.remove(os.path.abspath('media/sortimento/sortimsortemp.tmp'))
    except:
         pass
    
    first_run()
 
    return 'Ok'
a = multiprocessing.Process(target=resultado, daemon=True, name='resultado')

@app.after_request
def add_header(response):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    response.headers['Cache-Control'] = 'public, max-age=0'
    return response


@app.route('/', methods=['GET', 'POST'])
def home():
    print(a.is_alive(), b.is_alive(), c.is_alive(), d.is_alive(), e.is_alive(), f.is_alive(), g.is_alive(), h.is_alive())
    form = Secoes_form()
    sortimento, sortimento_arquivo, sort_criado = '', '', ''
    cruzada, cruzada_arquivo, cruzada_criado = '', '', ''
    cluster, cluster_arquivo, cluster_criado = '', '', ''
    for root, _, files in os.walk("media/sortimento/", topdown=False):
        for name in files:
            if name.endswith('.zip'):
                sortimento_arquivo = os.path.abspath(os.path.join(root, name))
                sortimento = '{}'.format(name)
                sort_criado = '{}'.format(
                    datetime.strftime(datetime.fromtimestamp(os.path.getmtime(os.path.join(root, name))),
                                      '%d/%m/%Y %H:%M'))
            elif name.endswith('.tmp'):
                sortimento_arquivo = ''
                sortimento = 'Loading'
                sort_criado = ''

    for root, dirs, files in os.walk("media/cruzada/", topdown=False):
        for name in files:
            if name.endswith('.zip'):
                cruzada_arquivo = os.path.abspath(os.path.join(root, name))
                cruzada = '{}'.format(name)
                cruzada_criado = '{}'.format(
                    datetime.strftime(datetime.fromtimestamp(os.path.getmtime(os.path.join(root, name))),
                                      '%d/%m/%Y %H:%M'))
            elif name.endswith('.tmp'):
                cruzada_arquivo = ''
                cruzada = 'Loading'
                cruzada_criado = ''

    for root, dirs, files in os.walk("media/cluster/", topdown=False):
        for name in files:
            if name.endswith('.zip'):
                cluster_arquivo = os.path.abspath(os.path.join(root, name))
                cluster = name
                cluster_criado = '{}'.format(
                    datetime.strftime(datetime.fromtimestamp(os.path.getmtime(os.path.join(root, name))),
                                      '%d/%m/%Y %H:%M'))
            elif name.endswith('.tmp'):
                cluster_arquivo = ''
                cluster = 'Loading'
                cluster_criado = ''

    conn = sqlite3.connect('apps/db/sortimento.db')
    cursor = conn.cursor()
    rows = cursor.execute("select id, tempo from PROCESSO ;").fetchall()
    conn.close()
    
    result = []
    for row in rows:
        # if row[0] == 3:
        #     result.append(response)
        # else:
        result.append(row)
    
    progress = []
    with db.engine.connect() as con:
        rs = con.execute('select id, progresso from processo;').fetchall()
        for item in rs:
            progress.append(item)

    return render_template('index.html', sortimento_arquivo=sortimento_arquivo, sortimento=sortimento,
                           sort_criado=sort_criado,
                           cruzada_arquivo=cruzada_arquivo, cruzada=cruzada, cruzada_criado=cruzada_criado,
                           cluster_arquivo=cluster_arquivo,
                           cluster=cluster, cluster_criado=cluster_criado, form=form, tempo=result, progress=progress)


@app.route('/sortimsortemp')
def sortimsortemp():
    global a
    for root, _, files in os.walk("media/sortimento", topdown=False):
        for name in files:
            os.remove(os.path.abspath(os.path.join(root, name)))
    ftmp = open(os.path.abspath('media/sortimento/sortimsortemp.tmp'), 'w+')
    ftmp.close()
    a = multiprocessing.Process(target=resultado, daemon=True, name='resultado')
    try:
        a.join()
        a.terminate()
    except:
        pass

    a.start()

    return redirect(url_for('home'))


@app.route('/downloadsortemp', methods=['GET', 'POST'])
def downloadsortemp():
    fi = ''
    for filename in glob.glob(os.path.join(os.path.abspath('media/sortimento'), '*.zip')):
        fi = os.path.abspath(f'{filename}')
        print(fi)
    f = os.path.abspath('media/sortimento/SortimSortemp.zip')
    return send_file(f'{fi}', mimetype='application/zip', as_attachment=True,
                     attachment_filename=f"{os.path.basename(fi)}.zip", max_age=0)


@app.route('/cruzada', methods=['GET', 'POST'])
def cruzada():
    try:
        a.join()
    except:
        pass
    form = Secoes_form()
    secao = form.secao.data
    secao_lista = str(secao).replace('[', '').replace(']', '').replace("'", '')
    print(secao_lista)
    if secao:
        secao = form.secao.data
        file = gerar_arquivos()
        for root, _, files in os.walk("media/cruzada", topdown=False):
            for name in files:
                os.remove(os.path.abspath(os.path.join(root, name)))
        ftmp = open(os.path.abspath('media/cruzada/cruzada.tmp'), 'w+')
        ftmp.close()

        file.query(sql.format(secao_lista, secao_lista), 'Base_cruzada', os.path.relpath('media/cruzada'),
                            tipo=1)

        file.zippar(str('Cruzada-{}'.format(secao_lista.replace(', ','-'))), os.path.relpath('media/cruzada'))
        os.remove(os.path.abspath('media/cruzada/cruzada.tmp'))
    return redirect(url_for('home'))


@app.route('/downloadcruzada', methods=['GET', 'POST'])
def downloadcruzada():
    fi=''
    for filename in glob.glob(os.path.join(os.path.abspath('media/cruzada'), '*.zip')):
        fi = os.path.abspath(f'{filename}')
    f = os.path.basename(fi)
    return send_file(f'{fi}', mimetype='application/zip', as_attachment=True, attachment_filename=f,
                     max_age=0)


@app.route('/cluster', methods=['GET', 'POST'])
def cluster():
    try:
        a.join()
    except:
        pass
    file = gerar_arquivos()
    for root, _, files in os.walk("media/cluster", topdown=False):
        for name in files:
            print(name)
            os.remove(os.path.abspath(os.path.join(root, name)))
    ftmp = open(os.path.abspath('media/cluster/cluster.tmp'), 'w+')
    ftmp.close()

    for root, dirs, files in os.walk("processos/sql_cluster_catalogo", topdown=False):
        for name in files:
            file.query(os.path.abspath(os.path.join(root, name)), Path(name).stem,
                                os.path.relpath('media/cluster'))
        file.zippar('Cluster_CatalagoVV+', os.path.relpath('media/cluster'))
    os.remove(os.path.abspath('media/cluster/cluster.tmp'))
    return redirect(url_for('home'))


@app.route('/downloadcluster', methods=['GET', 'POST'])
def downloadcluster():
    f = os.path.abspath('media/cluster/Cluster_CatalagoVV+.zip')
    return send_file(f'{f}', mimetype='application/zip', as_attachment=True,
                     attachment_filename="Cluster_CatalagoVV+.zip", max_age=0)


# _____________________________________________________ #


@app.route('/gerar', methods=['GET', 'POST'])
def gerar():
    id, concatenado, secoes, tabela, ordem, template = 1, 'CONCATENADO 2', perfil_tamanho_regiao, table_perfil_tamanho_regiao, ordem_PTR, 'Perfil_Tamanho_Regiao' 
    start = timeit.default_timer()
    with db.engine.connect() as con:
        rs = con.execute(f'select processando from processo where id = {id};')
        for item in rs.fetchone():
            status = item

        rs = con.execute(f'select atual from processo where id = {id};')
        for item in rs.fetchone():
            atual = item

        rs = con.execute(f'select progresso from processo where id = {id};')
        for item in rs.fetchone():
            progresso = item

    if status == 0:   
        global b     
        try:
            b.terminate()
            b.join()
        except:
            pass
        
        print(status)

        b = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar', daemon=True, args=(id, concatenado, secoes, tabela, ordem, template))
        b.start()

    elif status == 1:
        if b.is_alive():
            print(b.is_alive(), 'processando')
        else:
            try:
                b.terminate()
                b.join()
            except:
                pass
            finally:
                with db.engine.connect() as con:
                    con.execute(f"UPDATE PROCESSO SET processando = 0 where id = {id};")
                    con.execute(f"UPDATE PROCESSO SET atual = '' where id = {id};")
                    con.execute(f"UPDATE PROCESSO SET progresso = 100 where id = {id};")
        return render_template('loading_new.html', atual=atual, progresso=progresso)

    elif status == 2:
        for _, _, files in os.walk(f'apps/db/Arquivos{id}'):
            for name in files:
                if name.endswith('zip'):
                    return render_template('loading_finish.html')
            with db.engine.connect() as con:
                con.execute(f"UPDATE PROCESSO SET processando = 0 where id = {id};")
                con.execute(f"UPDATE PROCESSO SET atual = '' where id = {id};")
                con.execute(f"UPDATE PROCESSO SET progresso = 100 where id = {id};")
        try:
            if b.is_alive():
                b.terminate()
                b.join()

        except Exception as e:
            print(e)

        return redirect(url_for('gerar'))

    stop = timeit.default_timer()
    print((start-stop)/60/60)
    return render_template('loading_new.html', atual=atual, progresso=progresso)


@app.route('/downloadPRT', methods=['GET', 'POST'])
def downloadPRT():
    for root, _, files in os.walk('apps/db/Arquivos1'):
        for name in files:
            if name.endswith('zip'):
                print(os.path.abspath(os.path.join(root, name)))
                filepath = os.path.abspath(os.path.join(root, name))
                file = name

    return send_file(f'{filepath}', mimetype='application/zip', as_attachment=True, attachment_filename=f"{file}")


@app.route('/gerar1', methods=['GET', 'POST'])
def gerar1():    
    id, concatenado, secoes, tabela, ordem, template = 2, 'CONCATENADO 3', perfil_regiao, table_perfil_regiao, ordem_PR, 'Perfil_Regiao' 
    start = timeit.default_timer()
    with db.engine.connect() as con:
        rs = con.execute('select processando from processo where id = 2;')
        for item in rs.fetchone():
            status = item
            print(item)

        rs = con.execute('select atual from processo where id = 2;')
        for item in rs.fetchone():
            atual = item
            print(item)

        rs = con.execute('select progresso from processo where id = 2;')
        for item in rs.fetchone():
            progresso = item
            print(item)

    if status == 0:
        global c

        if c.is_alive():
            c.terminate()
            c.join()
        c = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar1', daemon=True, args=(id, concatenado, secoes, tabela, ordem, template))
        c.start()

    elif status == 1:
        if c.is_alive():
            print(c.is_alive(), 'processando')
        else:
            try:
                c.terminate()
                c.join()
            except:
                pass
            finally:
                with db.engine.connect() as con:
                    con.execute("UPDATE PROCESSO SET processando = 0 where id = 2;")
                    con.execute("UPDATE PROCESSO SET atual = '' where id = 2;")
                    con.execute("UPDATE PROCESSO SET progresso = 100 where id = 2;")
        return render_template('loading_new.html', atual=atual, progresso=progresso)


    elif status == 2:
        for _, _, files in os.walk(f'apps/db/Arquivos{id}'):
            for name in files:
                if name.endswith('zip'):
                    return render_template('loading_finish_1.html')
            with db.engine.connect() as con:
                con.execute("UPDATE PROCESSO SET processando = 0 where id = 2;")
                con.execute("UPDATE PROCESSO SET atual = '' where id = 2;")
                con.execute("UPDATE PROCESSO SET progresso = 100 where id = 2;")
        try:
            if c.is_alive():
                c.terminate()
                c.join()

        except Exception as e:
            print(e)

        return redirect(url_for('gerar1'))

    stop = timeit.default_timer()
    print((start - stop) / 60 / 60)
    return render_template('loading_new.html', atual=atual, progresso=progresso)


@app.route('/downloadPR', methods=['GET', 'POST'])
def downloadPR():
    for root, dirs, files in os.walk('apps/db/Arquivos2'):
        for name in files:
            if name.endswith('zip'):
                print(os.path.abspath(os.path.join(root, name)))
                filepath = os.path.abspath(os.path.join(root, name))
                file = name

    return send_file(f'{filepath}', mimetype='application/zip', as_attachment=True, attachment_filename=f"{file}")


@app.route('/gerar2', methods=['GET', 'POST'])
def gerar2():
    id, concatenado, secoes, tabela, ordem, template = 3, 'CONCATENADO', perfil_tamanho, table_perfil_tamanho, ordem_PT, 'Perfil_Tamanho' 
    start = timeit.default_timer()
    with db.engine.connect() as con:
        rs = con.execute('select processando from processo where id = 3;')
        for item in rs.fetchone():
            status = item
            print(item)

        rs = con.execute('select atual from processo where id = 3;')
        for item in rs.fetchone():
            atual = item
            print(item)

        rs = con.execute('select progresso from processo where id = 3;')
        for item in rs.fetchone():
            progresso = item
            print(item)

    if status == 0:
        global d
        
        if d.is_alive():
            print(d.is_alive())
            d.terminate()
            d.join()
        d = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar1', daemon=True, args=(id, concatenado, secoes, tabela, ordem, template))
        print('iniciando processo')
        
        d.start()

    elif status == 1:
        
        if d.is_alive():
            print(d.is_alive(), 'processando')
        else:
            try:
                d.terminate()
                d.join()
            except:
                pass
            finally:
                with db.engine.connect() as con:
                    con.execute("UPDATE PROCESSO SET processando = 0 where id = 3;")
                    con.execute("UPDATE PROCESSO SET atual = '' where id = 3;")
                    con.execute("UPDATE PROCESSO SET progresso = 100 where id = 3;")
        return render_template('loading_new.html', atual=atual, progresso=progresso)


    elif status == 2:
        for root, dirs, files in os.walk(f'apps/db/Arquivos{id}'):
            for name in files:
                if name.endswith('zip'):
                    return render_template('loading_finish_2.html')
            with db.engine.connect() as con:
                con.execute("UPDATE PROCESSO SET processando = 0 where id = 3;")
                con.execute("UPDATE PROCESSO SET atual = '' where id = 3;")
                con.execute("UPDATE PROCESSO SET progresso = 100 where id = 3;")
        try:
            if d.is_alive():
                d.terminate()
                d.join()

        except Exception as e:
            print(e)

        return redirect(url_for('gerar1'))

    stop = timeit.default_timer()
    print((start - stop) / 60 / 60)
    return render_template('loading_new.html', atual=atual, progresso=progresso)


@app.route('/downloadPT', methods=['GET', 'POST'])
def downloadPT():
    for root, dirs, files in os.walk('apps/db/Arquivos3'):
        for name in files:
            if name.endswith('zip'):
                print(os.path.abspath(os.path.join(root, name)))
                filepath = os.path.abspath(os.path.join(root, name))
                file = name

    return send_file(f'{filepath}', mimetype='application/zip', as_attachment=True, attachment_filename=f"{file}")


@app.route('/gerar3', methods=['GET', 'POST'])
def gerar3():
    id, concatenado, secoes, tabela, ordem, template = 4, 'CONCATENADO 5', bandeira_perfil_tamanho, table_bandeira_perfil_tamanho, ordem_BPT, 'Bandeira_Perfil_Tamanho' 
    start = timeit.default_timer()
    with db.engine.connect() as con:
        rs = con.execute('select processando from processo where id = 4;')
        for item in rs.fetchone():
            status = item
            print(item)

        rs = con.execute('select atual from processo where id = 4;')
        for item in rs.fetchone():
            atual = item
            print(item)

        rs = con.execute('select progresso from processo where id = 4;')
        for item in rs.fetchone():
            progresso = item
            print(item)


    if status == 0:
        global e

        if e.is_alive():
            print(e.is_alive())
            e.terminate()
            e.join()

        e = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar1', daemon=True, args=(id, concatenado, secoes, tabela, ordem, template))
        e.start()

    elif status == 1:
        
        if e.is_alive():
            print(e.is_alive(), 'processando')
        else:
            try:
                e.terminate()
                e.join()
            except:
                pass
            finally:
                with db.engine.connect() as con:
                    con.execute("UPDATE PROCESSO SET processando = 0 where id = 4;")
                    con.execute("UPDATE PROCESSO SET atual = '' where id = 4;")
                    con.execute("UPDATE PROCESSO SET progresso = 100 where id = 4;")
        return render_template('loading_new.html', atual=atual, progresso=progresso)


    elif status == 2:
        for root, dirs, files in os.walk(f'apps/db/Arquivos{id}'):
            for name in files:
                if name.endswith('zip'):
                    return render_template('loading_finish_3.html')
            with db.engine.connect() as con:
                con.execute("UPDATE PROCESSO SET processando = 0 where id = 4;")
                con.execute("UPDATE PROCESSO SET atual = '' where id = 4;")
                con.execute("UPDATE PROCESSO SET progresso = 100 where id = 4;")
        try:
            if e.is_alive():
                e.terminate()
                e.join()

        except Exception as e:
            print(e)

        return redirect(url_for('gerar3'))

    stop = timeit.default_timer()
    print((start-stop)/60/60)
    return render_template('loading_new.html', atual=atual, progresso=progresso)


@app.route('/downloadBFT', methods=['GET', 'POST'])
def downloadBPT():
    for root, dirs, files in os.walk('apps/db/Arquivos4'):
        for name in files:
            if name.endswith('zip'):
                print(os.path.abspath(os.path.join(root, name)))
                filepath = os.path.abspath(os.path.join(root, name))
                file = name

    return send_file(f'{filepath}', mimetype='application/zip', as_attachment=True, attachment_filename=f"{file}")


@app.route('/gerar4', methods=['GET', 'POST'])
def gerar4():
    tempo = ''
    start = timeit.default_timer()
    with db.engine.connect() as con:
        rs = con.execute('select processando from processo where id = 5;')
        for item in rs.fetchone():
            status = item
            print(item)

        rs = con.execute('select atual from processo where id = 5;')
        for item in rs.fetchone():
            atual = item
            print(item)

        rs = con.execute('select progresso from processo where id = 5;')
        for item in rs.fetchone():
            progresso = item
            print(item)


    if status == 0:
        global f

        if f.is_alive():
            print(f.is_alive())
            f.terminate()
            f.join()

        f = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar4', daemon=True)
        f.start()

    elif status == 1:
        
        if f.is_alive():
            print(f.is_alive(), 'processando')
        else:
            try:
                f.terminate()
                f.join()
            except:
                pass
            finally:
                with db.engine.connect() as con:
                    con.execute("UPDATE PROCESSO SET processando = 0 where id = 5;")
                    con.execute("UPDATE PROCESSO SET atual = '' where id = 5;")
                    con.execute("UPDATE PROCESSO SET progresso = 100 where id = 5;")
        return render_template('loading_new.html', atual=atual, progresso=progresso)


    elif status == 2:
        for root, dirs, files in os.walk('apps/db/Arquivos4'):
            for name in files:
                if name.endswith('zip'):
                    return render_template('loading_finish_4.html')
            with db.engine.connect() as con:
                con.execute("UPDATE PROCESSO SET processando = 0 where id = 5;")
                con.execute("UPDATE PROCESSO SET atual = '' where id = 5;")
                con.execute("UPDATE PROCESSO SET progresso = 100 where id = 5;")
        try:
            if f.is_alive():
                f.terminate()
                f.join()

        except Exception as e:
            print(e)

        return redirect(url_for('gerar4'))

    stop = timeit.default_timer()
    print((start-stop)/60/60)
    return render_template('loading_new.html', atual=atual, progresso=progresso)


@app.route('/downloadLT', methods=['GET', 'POST'])
def downloadLT():
    for root, dirs, files in os.walk('apps/db/Arquivos5'):
        for name in files:
            if name.endswith('zip'):
                print(os.path.abspath(os.path.join(root, name)))
                filepath = os.path.abspath(os.path.join(root, name))
                file = name

    return send_file(f'{filepath}', mimetype='application/zip', as_attachment=True, attachment_filename=f"{file}")


@app.route('/gerar5', methods=['GET', 'POST'])
def gerar5():
    id, concatenado, secoes, tabela, ordem, template = 6, 'CONCATENADO 6', bandeira_perfil_regiao, table_bandeira_perfil_regiao, ordem_BPR, 'Bandeira_Perfil_Regiao' 
    start = timeit.default_timer()
    with db.engine.connect() as con:
        rs = con.execute('select processando from processo where id = 6;')
        for item in rs.fetchone():
            status = item
            print(status)

        rs = con.execute('select atual from processo where id = 6;')
        for item in rs.fetchone():
            atual = item
            print(atual)

        rs = con.execute('select progresso from processo where id = 6;')
        for item in rs.fetchone():
            progresso = item
            print(progresso)


    if status == 0:
        global g
        try:
            g.terminate()
            g.join()
        except:
            pass
        g = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar1', daemon=True, args=(id, concatenado, secoes, tabela, ordem, template))
        g.start()

    elif status == 1:
        if g.is_alive():
            print(g.is_alive(), 'processando')
        else:
            try:
                g.terminate()
                g.join()
            except:
                pass
            finally:
                with db.engine.connect() as con:
                    con.execute("UPDATE PROCESSO SET processando = 0 where id = 6;")
                    con.execute("UPDATE PROCESSO SET atual = '' where id = 6;")
                    con.execute("UPDATE PROCESSO SET progresso = 100 where id = 6;")
        return render_template('loading_new.html', atual=atual, progresso=progresso)


    elif status == 2:
        for root, dirs, files in os.walk(f'apps/db/Arquivos{id}'):
            for name in files:
                if name.endswith('zip'):
                    return render_template('loading_finish_5.html')
            with db.engine.connect() as con:
                con.execute("UPDATE PROCESSO SET processando = 0 where id = 6;")
                con.execute("UPDATE PROCESSO SET atual = '' where id = 6;")
                con.execute("UPDATE PROCESSO SET progresso = 100 where id = 6;")
        try:
            if g.is_alive():
                g.terminate()
                g.join()

        except Exception as e:
            print(e)

        return redirect(url_for('gerar5'))

    stop = timeit.default_timer()
    print((start-stop)/60/60)
    return render_template('loading_new.html', atual=atual, progresso=progresso)


@app.route('/downloadBPR', methods=['GET', 'POST'])
def downloadBPR():
    for root, dirs, files in os.walk('apps/db/Arquivos6'):
        for name in files:
            if name.endswith('zip'):
                print(os.path.abspath(os.path.join(root, name)))
                filepath = os.path.abspath(os.path.join(root, name))
                file = name

    return send_file(f'{filepath}', mimetype='application/zip', as_attachment=True, attachment_filename=f"{file}")


@app.route('/gerar6', methods=['GET', 'POST'])
def gerar6():
    id, concatenado, secoes, tabela, ordem, template = 7, 'CONCATENADO 7', telefonia, table_telefonia, ordem_TEL, 'Telefonia' 
    start = timeit.default_timer()
    with db.engine.connect() as con:
        rs = con.execute('select processando from processo where id = 7;')
        for item in rs.fetchone():
            status = item
            print(status)

        rs = con.execute('select atual from processo where id = 7;')
        for item in rs.fetchone():
            atual = item
            print(atual)

        rs = con.execute('select progresso from processo where id = 7;')
        for item in rs.fetchone():
            progresso = item
            print(progresso)


    if status == 0:
        global h
        try:
            h.terminate()
            h.join()
        except:
            pass
        h = multiprocessing.Process(target=criar_campo_minado.processar, name='gerar1', daemon=True, args=(id, concatenado, secoes, tabela, ordem, template))
        h.start()

    elif status == 1:
        if h.is_alive():
            print(h.is_alive(), 'processando')
        else:
            try:
                h.terminate()
                h.join()
            except:
                pass
            finally:
                with db.engine.connect() as con:
                    con.execute("UPDATE PROCESSO SET processando = 0 where id = 7;")
                    con.execute("UPDATE PROCESSO SET atual = '' where id = 7;")
                    con.execute("UPDATE PROCESSO SET progresso = 100 where id = 7;")
        return render_template('loading_new.html', atual=atual, progresso=progresso)


    elif status == 2:
        for root, dirs, files in os.walk(f'apps/db/Arquivos{id}'):
            for name in files:
                if name.endswith('zip'):
                    return render_template('loading_finish_6.html')
            with db.engine.connect() as con:
                con.execute("UPDATE PROCESSO SET processando = 0 where id = 7;")
                con.execute("UPDATE PROCESSO SET atual = '' where id = 7;")
                con.execute("UPDATE PROCESSO SET progresso = 100 where id = 7;")
        try:
            if h.is_alive():
                h.terminate()
                h.join()

        except Exception as e:
            print(e)

        return redirect(url_for('gerar6'))

    stop = timeit.default_timer()
    print((start-stop)/60/60)
    return render_template('loading_new.html', atual=atual, progresso=progresso)


@app.route('/downloadTEL', methods=['GET', 'POST'])
def downloadTEL():
    for root, dirs, files in os.walk('apps/db/Arquivos7'):
        for name in files:
            if name.endswith('zip'):
                print(os.path.abspath(os.path.join(root, name)))
                filepath = os.path.abspath(os.path.join(root, name))
                file = name

    return send_file(f'{filepath}', mimetype='application/zip', as_attachment=True, attachment_filename=f"{file}")


@app.route('/reset/<int:id>', methods=['GET', 'POST'])
def reset(id):
    with db.engine.connect() as con:
        con.execute(f"UPDATE PROCESSO SET processando = 0 where id = {id};")
        con.execute(f"UPDATE PROCESSO SET atual = '' where id = {id};")
        con.execute(f"UPDATE PROCESSO SET progresso = '100' where id = {id};")
        con.execute(f"UPDATE PROCESSO SET tempo = '0' where id = {id};")
    id = int(id)
    try:
        a.join()
    except:
        pass
    if id == 1:        
        response = requests.get(f'http://localhost:{port}/gerar')
    else:
        response = requests.get(f'http://localhost:{port}/gerar{id-1}')
        
    return redirect(url_for('home'))

@app.route('/restart')
def restart():
    with db.engine.connect() as con:
        con.execute("UPDATE PROCESSO SET processando = 0 where id in (1,2,3,4,5,6,7);")
        con.execute("UPDATE PROCESSO SET atual = '' where id in (1,2,3,4,5,6,7);")
        con.execute("UPDATE PROCESSO SET progresso = '100' where id in (1,2,3,4,5,6,7);")
        con.execute("UPDATE PROCESSO SET tempo = '0' where id in (1,2,3,4,5,6,7);")
    try:
        a.join()
        b.terminate()
        c.terminate()
        d.terminate()
        e.terminate()
        f.terminate()
        g.terminate()
    except:
        pass
    try:
        os.system("taskkill /f /im  EXCEL.exe")
        
    except:
        pass
            
    return redirect(url_for('home'))
@app.route('/killjoin')
def killjoin():
    
    try:
        a.join()
        a.terminate()
        b.terminate()
        c.terminate()
        d.terminate()
        e.terminate()
        f.terminate()
        g.terminate()
    except:
        pass
    try:
        os.system("taskkill /f /im  EXCEL.exe")
        
    except:
        pass
            
    return redirect(url_for('home'))




def disparar():
    response2 = requests.get('http://localhost:{port}/sortimsortemp')
    print('disparando sortim/sortemp')
def disparar_1():
    response2 = requests.get('http://localhost:{port}/gerar')
def disparar_2():
    response3 = requests.get('http://localhost:{port}/gerar1')
def disparar_3():    
    response4 = requests.get('http://localhost:{port}/gerar2')
def disparar_4():
    response5 = requests.get('http://localhost:{port}/gerar3')
def disparar_5():
    response6 = requests.get('http://localhost:{port}/gerar6')
def disparar_6():
    response7 = requests.get('http://localhost:{port}/gerar5')

first_run()

if __name__ == '__main__':
    a = multiprocessing.Process(target=resultado, daemon=True, name='resultado')
    p = multiprocessing.Process(target=first_run)
    job_start = scheduler.add_job(first_run, 'cron', day_of_week='*', hour=3, minute=59, start_date='2020-09-04')
    job = scheduler.add_job(disparar, 'cron', day_of_week='*', hour=4, minute=00, start_date='2020-09-04')
    job1 = scheduler.add_job(disparar_1, 'cron', day_of_week='*', hour=4, minute=30, start_date='2020-09-04')
    job2 = scheduler.add_job(disparar_2, 'cron', day_of_week='*', hour=5, minute=10, start_date='2020-09-04')
    job3 = scheduler.add_job(disparar_4, 'cron', day_of_week='*', hour=5, minute=20, start_date='2020-09-04')
    job4 = scheduler.add_job(disparar_5, 'cron', day_of_week='*', hour=5, minute=30, start_date='2020-09-04')
    job5 = scheduler.add_job(disparar_6, 'cron', day_of_week='*', hour=5, minute=40, start_date='2020-09-04')
    job3 = scheduler.add_job(disparar_3, 'cron', day_of_week='*', hour=4, minute=55, start_date='2020-09-04')
    scheduler.start()

    #app.run(host='0.0.0.0', port=port, threaded=True)
    #uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
    #serve(app, host='0.0.0.0', port=80, log_socket_errors=True, threads=6)
    
