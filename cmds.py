############
#SQL Comands
############
import os,json,shutil,csv
from Errors import *

class read_scheme(object):
    
    def __init__(self,filename):
        reader = json.load(open(filename+'.json','r'))
        self.fields = reader["schema"]
        self.nameIndexDict ={field["field"] : i for i,field in enumerate(self.fields)}

    def index_by_field(self,field):
        return self.nameIndexDict[field]

    def type_by_field(self,field):
        return self.fields[self.nameIndexDict[field]]["type"]

class select(object):
    def __init__(self,fields,file_name,origin,where_cond,group_field,group_cond,order_fields):
        os.chdir(origin)
        self.old_scheme = read_scheme(origin)
        self.fields = {field : self.old_scheme.type_by_field(field) for field in fields}
        self.origin = origin
        self.name = file_name
        if where_cond:
            self.where_func = lambda item : eval("{} {} {}".format(item,where_cond[1],where_cond[2]))
            self.where_field = where_cond[0]
        else:
            self.where_func = lambda x : True
            self.where_field = self.old_scheme.fields[0]['field'] #default value will be the first field
        if group_field or group_cond or order_fields:
            raise NotImplementedError

    def table_by_rules(self):
        raw_table = csv.reader(open(self.origin + '.zis'))
        self.table = []
        needed_fields = {field : self.old_scheme.index_by_field(field) for field in self.fields}
        for row in raw_table:
            if not self.where_func(row[needed_fields[self.where_field]]): continue
            row_to_add = [row[i] for i in needed_fields.values()]
            self.table.append(row_to_add)
    
    def create_file(self,file_name): 
        if os.path.exists(os.path.join(os.getcwd(), file_name)):
            raise TableExistsError(file_name)
        os.mkdir(file_name)
        os.chdir(file_name)
        s = open(file_name + '.zis',"w")
        for row in self.table:
            s.writeline(", ".join(row))
        s.close()
        scheme = [{'field':field,'type':typ} for field,typ in self.fields.items()]
        json.dump({'schema': scheme },open(file_name+'.json','w'),indent=4)
        os.chdir('..')

    def print_table(self):
        for i,row in enumerate(self.table):
            if i > 100: break
            for item in row:
                if item != row[-1]: print(item,end=',')
                else: print(item)

    def execute(self):
        self.table_by_rules()
        os.chdir('..')
        if self.name:
            self.create_file(self.name)
        self.print_table()

class load(object):
    def __init__(self,origin,name,ignoring,fields=['*']):
        os.chdir(origin)
        self.origin = origin
        self.raw_table = csv.reader(open(self.origin + '.zis'))
        print("Table {}.zis loaded".format(self.origin))
        self.raw_scheme = read_scheme(self.origin)
        print("scheme {}.json loaded".format(self.origin))
        self.name = name
        self.ignoring = ignoring
        self.fields = fields

    def loader(self):
        os.chdir('..')
        if os.path.exists(self.name): raise TableExistsError(self.name)
        os.mkdir(self.name)
        os.chdir(self.name)
        self.table = []
        self.scheme = []
        if self.fields != ['*']:
            for row in self.raw_table:
                row_to_add = [row[i] for i in [self.raw_scheme.index_by_field(field) for field in self.fields]]
                self.table.append(row_to_add)
            self.scheme = [{'field' : field,'type': \
    self.raw_scheme.fields[self.raw_scheme.index_by_field(field)]['type']} for field in self.fields]
        else:
            self.table = [row for i, row in enumerate(self.raw_table) if self.ignoring <= i]
            self.scheme = self.raw_scheme.fields
        
    def creator(self):
        s = open(self.name+'.zis',"w")
        for row in self.table:
            s.write(", ".join(row))
            s.write('\n')
        s.close()
        json.dump({'schema': self.scheme},open(self.name+'.json','w'),indent=4)
        
    def execute(self):
        self.loader()
        self.creator()
        print("Table {}.zis was loaded from {}.zis without {} rows".format(self.name,self.origin,self.ignoring))
        os.chdir('..')

class create(object):
    def __init__(self,table_name,if_not_exists,fields):
        self.name = table_name
        self.ine = if_not_exists
        self.fields = fields


    def create_table(self):
        if os.path.exists(self.name):
            if self.ine: return
            if not self.ine: raise TableExistsError(self.name)
        os.mkdir(self.name)
        os.chdir(self.name)
        s = open(self.name + '.zis',"w")
        s.write(", ".join(self.fields.keys()))
        s.close()    
    
    def create_scheme(self):
        res = [{'field':field,'type':typ} for field,typ in self.fields.items()]
        json.dump({'schema': res },open(self.name+'.json','w'),indent=4)

    def execute(self):
        self.create_table()
        print('Table {}.zis created'.format(self.name))
        self.create_scheme()
        print('scheme {}.json created'.format(self.name))
        os.chdir('..')

        
class drop(object):
    def __init__(self,table_name,if_exists):
        self.ie = if_exists
        self.name = table_name

    def drop_table(self):
        if os.path.exists(self.name):
            shutil.rmtree(self.name)
            print("the table {} has been deleted".format(self.name))
        elif not self.ie:
            raise TableDoesNotExistError(self.name)

    def execute(self):
        self.drop_table()