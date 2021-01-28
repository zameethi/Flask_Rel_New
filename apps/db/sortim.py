from .conexao import *

db.Model.metadata.reflect(db.engine)


class Sortim(db.Model):
    __tablename__ = 'sortim'
    __table_args__ = {'extend_existing': True}
    index = db.Column(db.Integer, primary_key=True)
    CODPUBLICO = db.Column(db.TEXT)
    DESCPUBLICO = db.Column(db.TEXT)
    CODSECAO = db.Column(db.TEXT)
    DESCSECAO = db.Column(db.TEXT)
    CODSORTIMENTO = db.Column(db.TEXT)
    CODTAMANHO = db.Column(db.TEXT)
    DESCTAMANHO = db.Column(db.TEXT)
    EMPRESA = db.Column(db.TEXT)
    FILIAL = db.Column(db.TEXT)
    TIPOPUBLICO = db.Column(db.TEXT)
    SOLICVOLTDIF = db.Column(db.TEXT)
    VOLTAGEM = db.Column(db.TEXT)
