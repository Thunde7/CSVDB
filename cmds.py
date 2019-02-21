############
#SQL Comands
############
import os,json,shutil,csv,reader
from Errors import TableDoesNotExistError,TableExistsError
from table import Table
from columm import Columm


class select(object):
    def __init__(self,fields,file_name,origin,where_cond,group_field,group_cond,order_fields):
        self.old_table = reader.reader(origin)
        self.old_scheme = reader.read_scheme(origin)
        if fields != "*":
            self.fields = {field : self.old_scheme.type_by_field(field) for field in fields} 
        else:
            self.fields = {item['field']:item['type'] for item in self.old_scheme.fields}
        self.origin = origin
        self.name = file_name
        self.get_where(where_cond)
        self.order = order_fields
        if group_field or group_cond:
            raise NotImplementedError
        self.file_name = file_name
        
    def get_where(self,where_cond):
        if where_cond == []: self.where_lines = [i for i in range(self.old_table.length)]; return  
        constant = where_cond[2] if where_cond[2] != 'NULL' or self.fields[where_cond[0]] != 'varchar' else '' 
        where_dict = {
        '<': lambda item : item != 'NULL' and item < constant,
        '=': lambda item : item != 'NULL' and item == constant,
        '>': lambda item : item != 'NULL' and item > constant,
        '<>': lambda item : item != 'NULL' and item != constant,
        '<=': lambda item : item != 'NULL' and item <= constant,
        '>=': lambda item : item != 'NULL' and item >= constant,
        'is': lambda item : item is constant,
        'is not': lambda item : item is not constant,
        }
        where_func = where_dict[where_cond[1]]
        self.where_lines = self.old_table.columms[where_cond[0]].where(where_func)

    def table_by_rules(self):
        needed_columms = [self.old_table.columms[field] for field in self.fields.keys()]
        self.new_columms = [[] for _ in needed_columms]
        for line in self.where_lines:
            for i in range(len(needed_columms)):
                self.new_columms[i].append(needed_columms[i][line])
        table_columms = [Columm(list(self.fields.keys())[i],list(self.fields.values())[i],self.new_columms[i])\
                                             for i in range(len(self.fields))]
        self.table = Table(table_columms)
        self.table.order(self.order)
        

    def create_file(self,file_name): 
        if os.path.exists(os.path.join(os.getcwd(), file_name)):
            raise TableExistsError(file_name)
        os.mkdir(file_name)
        os.chdir(file_name)
        reader.write(file_name,self.table)
        scheme = [{'field':field,'type':typ} for field,typ in self.fields.items()]
        json.dump({'schema': scheme },open(file_name+'.json','w'),indent=4)
        os.chdir('..')

    def execute(self,verbose):
        self.table_by_rules()
        if verbose:
            print('table {} created by select command'.format(self.name))
        if self.name:
            self.create_file(self.name)
        if verbose:
            print(self.table)

class load(object):
    def __init__(self,origin,name,ignoring):
        self.origin = origin
        self.raw_table = reader.reader(self.origin)
        print("Table {}.zis loaded".format(self.origin))
        self.scheme = reader.read_scheme(self.origin)
        print("scheme {}.json loaded".format(self.origin))
        self.name = name
        self.ignoring = ignoring

    def loader(self):
        if os.path.exists(self.name): raise TableExistsError(self.name)
        os.mkdir(self.name)
        os.chdir(self.name)
        reader.write(self.name,self.raw_table,self.ignoring)
        
        json.dump({'schema': self.scheme.fields},open(self.name+'.json','w'),indent=4)
        os.chdir('..')
        
    def execute(self, verbose):
        self.loader()
        if verbose:
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
        reader.write(self.name,self.fields.values())   
    
    def create_scheme(self):
        res = [{'field':field,'type':typ} for field,typ in self.fields.items()]
        json.dump({'schema': res },open(self.name+'.json','w'),indent=4)

    def execute(self,verbose):
        self.create_table()
        if verbose:
            print('Table {}.zis created'.format(self.name))
        self.create_scheme()
        if verbose:
            print('scheme {}.json created'.format(self.name))
        os.chdir('..')

        
class drop(object):
    def __init__(self,table_name,if_exists):
        self.ie = if_exists
        self.name = table_name

    def drop_table(self):
        if os.path.exists(self.name):
            shutil.rmtree(self.name)
        elif not self.ie:
            raise TableDoesNotExistError(self.name)

    def execute(self,verbose):
        self.drop_table()
        if verbose:
            print("the table {} has been deleted".format(self.name))