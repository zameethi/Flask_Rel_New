from .conexao import *

db.Model.metadata.reflect(db.engine)


class Sortemp(db.Model):
    __tablename__ = 'sortemp'
    __table_args__ = {'extend_existing': True}
    CODSORTIMENTO = db.Column(db.Integer, primary_key=True)
    CODSECAO = db.Column(db.TEXT)
    DESCSECAO = db.Column(db.TEXT)
    CODTAMANHO = db.Column(db.TEXT)
    DESCTAMANHO = db.Column(db.TEXT)
    CODPUBLICO = db.Column(db.TEXT)
    DESCPUBLICO = db.Column(db.TEXT)
    CODMERCADORIA = db.Column(db.TEXT)
    DESCMERCADORIA = db.Column(db.TEXT)
    MERCCONJ = db.Column(db.TEXT)
    PRIORIDADE = db.Column(db.TEXT)
    QTDE = db.Column(db.TEXT)
    VOLTAGEM = db.Column(db.TEXT)
    VIGENCIAINICIO = db.Column(db.TEXT)
    VIGENCIAFIM = db.Column(db.TEXT)
    STATUS = db.Column(db.TEXT)
    SETOR = db.Column(db.TEXT)
    DESCSETOR = db.Column(db.TEXT)
    CLASSE = db.Column(db.TEXT)
    DESCCLASSE = db.Column(db.TEXT)
    ESPECIE = db.Column(db.TEXT)
    DESCESPECIE = db.Column(db.TEXT)
    MARCA = db.Column(db.TEXT)
    DESCMARCA = db.Column(db.TEXT)