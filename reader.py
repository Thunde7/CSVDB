from columm import Columm
from table import Table
import csv,os,json

class read_scheme(object):
    def __init__(self,filename):
        reader = json.load(open(filename+'.json','r'))

        self.fields = reader["schema"]
        self.nameIndexDict = {field["field"] : i for i,field in enumerate(self.fields)}
        os.chdir('..')

    def index_by_field(self,field):
        return self.nameIndexDict[field]

    def type_by_field(self,field):
        print(field + ": " + self.fields[self.nameIndexDict[field]]["type"])

        return self.fields[self.nameIndexDict[field]]["type"]


def reader(filename, ignoring):
#    os.chdir(os.path.sep.join(os.path.sep.split(filename)[:-1]))
    scheme = read_scheme(filename)
    if not os.path.exists(filename + '.zis'):
        fil = csv.reader(open(filename+ '.csv'))
    else:
        fil = csv.reader(open(filename + '.zis')) #need to implement pulke ish
    for i,line in enumerate(fil):
        if i == 0: 
            columms = [[title] for title in line]
        elif ignoring <= i:
            for j,item in enumerate(line):
                columms[j].append(item)
                print(str(i) + " " + item)
            
    columnlist = []
    print(columms)
    for item in columms:
        print('len of item is ' + str(len(item)))
        if len(item) > 1:
            columnlist.append(Columm(item[0],scheme.type_by_field(item[0]),item[1:]))
        else:
            columnlist.append(Columm(item[0],scheme.type_by_field(item[0]),[]))
    table = Table(columnlist)
    os.chdir('..')
    return table


def write(filename,table,ignoring = 0):
    fil = open(filename + '.zis','w')
    for i,row in enumerate(table):
        if ignoring <= i:
            fil.write(','.join(row))
            fil.write('\n')
