############
#SQL Comands
############
import os,json,shutil,csv,reader
from Errors import TableDoesNotExistError,TableExistsError
from table import Table
from column import Column
FILEENDING = ".zis"

class select(object):
    def __init__(self,fields,file_name,origin,where_cond,group_field,group_cond,order_fields):
        self.old_table = reader.reader(origin, False)
        self.old_scheme = reader.read_scheme(origin)
        if fields != "*":
            self.fields = {field : self.old_scheme.type_by_field(field) for field in fields}
        else:
            self.fields = {item['field']:item['type'] for item in self.old_scheme.fields}
        self.origin = origin
        self.name = file_name
        if where_cond != []:
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
        self.where_lines = self.old_table.columns[where_cond[0]].where(where_func)

    def table_by_rules(self):
        needed_columns = [self.old_table.columns[field] for field in self.fields.keys()]
        self.new_columns = [[] for _ in needed_columns]
        for line in self.where_lines:
            for i in range(len(needed_columns)):
                self.new_columns[i].append(needed_columns[i][line])
        table_columns = [Column(list(self.fields.keys())[i],list(self.fields.values())[i],self.new_columns[i])\
                                             for i in range(len(self.fields))]
        self.table = Table(table_columns)
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
    global FILEENDING
    def __init__(self,origin,name,ignoring):
        if os.path.isdir(os.getcwd() + os.path.sep + origin.split(".")[0]):
            self.origin = os.getcwd() + os.path.sep + origin.split(".")[0] + os.path.sep + origin
        else:
            self.origin = os.getcwd() + os.path.sep + origin
        self.name = name
        self.ignoring = ignoring

    def loader(self):
        with open(self.origin,"r") as f:
            dest = open("." + os.path.sep + self.name + os.path.sep + self.name  + FILEENDING,"a+")
            for _ in range(self.ignoring):
                f.readline()
            nextLine = f.readline()
            while nextLine:
                dest.write(nextLine + "\n")
                nextLine = f.readline()
        f.close()
        dest.close()


    def execute(self, verbose):
        self.loader()
        if verbose:
            print(f"Table {self.origin} was loaded into {self.name} without {self.ignoring} rows")

class create(object):
    def __init__(self,table_name,if_not_exists,fields):
        self.name = table_name
        self.ine = if_not_exists
        self.fields = fields

    def create_table(self):
        if os.path.exists(self.name):
            if self.ine: return
            else: raise TableExistsError(self.name)
        self.table = Table([Column(header,typ,[]) for (header,typ) in list(self.fields.items())])
        print("=====================TABLE======================\n", self.table)

        os.mkdir(self.name)
        os.chdir(self.name)
        reader.write(self.name,self.table)

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
