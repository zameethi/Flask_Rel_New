from .conexao import *

db.Model.metadata.reflect(db.engine)


class Secao(db.Model):
    __tablename__ = 'secoes'
    __table_args__ = {'extend_existing': True}
    PERFIL = db.Column(db.TEXT)
    SECAO = db.Column(db.INTEGER, primary_key=True)
    TAMANHO = db.Column(db.TEXT)
    OBRIGATORIO = db.Column(db.INTEGER)
    SUGESTAO = db.Column(db.INTEGER)
