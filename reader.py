from columm import Columm
from table import Table
import csv,os,json

class read_scheme(object):
    def __init__(self,filename):
        reader = json.load(open(filename+'.json','r'))
        self.fields = reader["schema"]
        self.nameIndexDict = {field["field"] : i for i,field in enumerate(self.fields)}

    def index_by_field(self,field):
        return self.nameIndexDict[field]

    def type_by_field(self,field):
        return self.fields[self.nameIndexDict[field]]["type"]


def reader(filename):
    assert(os.path.exists(filename))
    os.chdir(filename)
    scheme = read_scheme(filename)
    if not os.path.exists(filename + '.zis'):
        fil = csv.reader(open(filename))
    else: fil = csv.reader(open(filename + '.zis')) #need to implement pulke ish
    for i,line in enumerate(fil):
        if i == 0: columms = [[] for _ in line]
        for j,item in enumerate(line):
            columms[j].append(item)
    table = Table([Columm(item[0],scheme.type_by_field(item[0]),item[1:]) for item in columms])
    os.chdir('..')
    return table


def write(filename,table,ignoring = 0):
    os.chdir(filename)
    fil = open(filename + '.zis','w')
    for i,row in enumerate(table):
        if ignoring <= i:
            fil.write(row)
            fil.write('\n')
    

