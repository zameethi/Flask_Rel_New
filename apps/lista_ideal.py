import csv
import os

def listar_ideal():
    path = os.getcwd()

    with open(f'{path}/apps/ideal.csv',mode='r',newline='') as f:
        csvfile = csv.reader(f, delimiter=';')
        dict_ideal = {}
        for row in csvfile:
            temp = []
            for n, item in enumerate(row):
                if n == 0:
                    pass
                else:
                    temp.append(item)
            dict_ideal[int(row[0])] = temp
    return dict_ideal
