from column import Column
from table import Table
import csv,os,json

class read_scheme(object):
    def __init__(self,filename):
        os.chdir(filename)
        reader = json.load(open(filename+'.json','r'))
        os.chdir("..")
        self.fields = reader["schema"]
        self.nameIndexDict = {field["field"] : i for i,field in enumerate(self.fields)}
        

    def index_by_field(self,field):
        return self.nameIndexDict[field]

    def type_by_field(self,field):
        print(f"{field}:{self.fields[self.nameIndexDict[field]]['type']}")

        return self.fields[self.nameIndexDict[field]]["type"]


def reader(filename, ignoring,scheme=None):
    print(filename)
    #os.chdir(os.path.sep.join(os.path.sep.split(filename)[:-1]))
    os.chdir(filename)
    print(os.getcwd())
    print(os.listdir("."))
    if scheme is None:
        try:
            scheme = read_scheme(filename)
        except:
            scheme = {}
    if not os.path.exists(filename + '.zis'):
        fil = csv.reader(open(filename+ '.csv'))
    else:   
        fil = csv.reader(open(filename + '.zis')) #need to implement pulke ish
    for i,line in enumerate(fil):
        if i == 0: 
            columns = [[title] for title in line]
        elif ignoring <= i:
            for j,item in enumerate(line):
                columns[j].append(item)
                
    columnlist = []
    if scheme != {}:
        for item in columns:
            print('len of item is ' + str(len(item)))
            if len(item) > 1:
                columnlist.append(Column(item[0],scheme.type_by_field(item[0]),item[1:]))
            else:
                columnlist.append(Column(item[0],scheme.type_by_field(item[0]),[]))
    
        print(columnlist)
        table = Table(columnlist)
        os.chdir("..")
        return table
    os.chdir("..")    
    return columnlist


def write(filename,table,ignoring = -1):
    if type(table) == list:
        fil = open(filename + '.zis','w+')
        for i,row in [[str(col[i]) for col in table] for i in range(len(table[0]))]:
            if ignoring <= i:
                fil.write(','.join(row))
                fil.write('\n')
    else:
        fil = open(filename + '.zis','w+')
        for i,row in enumerate(table):
            if ignoring <= i:
                fil.write(','.join(row))
                fil.write('\n')
    fil.close()