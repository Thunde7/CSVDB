import sqltokenizer as token
import json,os,csv

path = os.getcwd()
if not os.path.exists('DBS'): os.mkdir("DBS")
dbspath = os.path.join(path,'DBS')
os.chdir(dbspath)


class TableExistsError(Exception):
    def __init(self,name):
        self.message = 'the table {} already exists'.format(name)

class CSVDBSyntaxError(ValueError):
    def __init__(self, message, line, col, text):
        super().__init__()
        self.line = line
        self.col = col
        self.text = text
        self.message = "CSVDB Syntax error at line {}  col {}: {}".format(line, col, message)

    def show_error_location(self):
        """Returns a string with the original string and the location of the syntax error"""
        s = ""
        for i, line_text in enumerate(self.text.splitlines() + ["\n"]):
            s += line_text
            if i == self.line:
                s += "=" * (self.col - 1) + "^^^\n"
        return s

    def __str__(self):
        return self.message


class Tok(object):
    def __init__(self,kind,val):
        self.kind = kind
        self.val = val

    def __repr__(self):
        return "{}:{}".format(self.kind,self.val)

    def __eq__(self,other):
        return self.kind == other.kind and self.val == other.val
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
        if where_cond or group_field or group_cond or order_fields:
            raise NotImplementedError
        self.table_by_rules()
        os.chdir('..')
        if file_name:
            self.create_file(file_name)
        self.print_table()

    def table_by_rules(self):
        raw_table = csv.reader(open(self.origin + '.zis'))
        self.table = []
        for row in raw_table:
            row_to_add = [row[i] for i in [self.old_scheme.index_by_field(field) for field in self.fields]]
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

class load(object):
    def __init__(self,origin,name,ignoring,fields=['*']):
        os.chdir(origin)
        self.origin = origin
        self.raw_table = csv.reader(open(self.origin + '.zis'))
        print("Table {}.zis loaded".format(self.origin))
        self.raw_scheme = raw_scheme = read_scheme(self.origin)
        print("scheme {}.json loaded".format(self.origin))
        self.name = name
        self.ignoring = ignoring
        self.loader(fields)
        self.creator()
        print("Table {}.zis was loaded from {}.zis without {} rows".format(self.name,self.origin,self.ignoring))
        os.chdir('..')

    def loader(self,fields):
        os.chdir('..')
        if os.path.exists(self.name): raise TableExistsError(self.name)
        os.mkdir(self.name)
        os.chdir(self.name)
        self.table = []
        self.scheme = []
        if fields != ['*']:
            for row in self.raw_table:
                row_to_add = [row[i] for i in [self.raw_scheme.index_by_field(field) for field in fields]]
                self.table.append(row_to_add)
            self.scheme = [{'field' : field,'type': \
    self.raw_scheme.fields[self.raw_scheme.index_by_field(field)]['type']} for field in fields]
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
        
class create(object):
    def __init__(self,table_name,if_not_exists,fields):
        self.name = table_name
        self.ine = if_not_exists
        self.fields = fields
        self.create_table()
        print('Table {}.zis created'.format(self.name))
        self.create_scheme()
        print('scheme {}.json created'.format(self.name))
        os.chdir('..')

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

class parser(object):
    
    def __init__(self,text):
        self.token = token.SqlTokenizer(text)
        self.tokens = []
        self._index = 0
        while True:
            tok, val = self.token.next_token()
            self.tokens.append(Tok(tok,val))
            if tok == token.SqlTokenKind.EOF:
                break
        print("text processed")
        if self.cur_val() == 'load':
            self.step()
            self.load()
        if self.cur_val() == 'drop':
            self.step()
            self.drop()
        if self.cur_val() == 'create':
            self.step()
            self.create()
        if self.cur_val() == 'select':
            self.step()
            self.select()
        
    def step(self,n=1):
        self._index += n

    def cur_val(self):
        return self.tokens[self._index].val
    
    def cur_kind(self):
        return self.tokens[self._index].kind

    def create(self):
        assert(self.cur_val() == 'table')
        self.step()
        if_not_exists = False
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'if'):
            self.step()
            assert(self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'not'))
            self.step()
            assert(self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'exists'))
            if_not_exists == True
            self.step()
        assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
        table_name = self.cur_val()
        self.step()
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'as'):
            self.step()
            self.createAS(table_name,if_not_exists)
        assert(self.cur_val() == '(')
        self.step()
        assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
        f = self.cur_val()
        self.step()
        assert(self.cur_kind() == token.SqlTokenKind.KEYWORD)
        t = self.cur_val()
        fields = {f:t}
        self.step()
        while self.cur_kind() != token.SqlTokenKind.OPERATOR or self.cur_val() != ')':
            if self.cur_val() == "," : self.step()
            assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
            fiel = self.cur_val()
            self.step()
            assert(self.cur_kind() == token.SqlTokenKind.KEYWORD)
            typ = self.cur_val()
            fields[fiel] = typ
            self.step()
        self.step()
        assert(self.cur_val() == None or self.cur_val() == ';')
        create(table_name,if_not_exists,fields)            
    
    def load(self):
        assert(self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'data'))
        self.step()
        assert(self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'infile'))
        self.step()
        assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
        origin = self.cur_val()
        self.step()
        assert(self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'into'))
        self.step()
        assert(self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'table'))
        self.step()
        assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
        table_name = self.cur_val()
        self.step()
        ignoring = 0
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'ignore'):
            self.step()
            assert(self.cur_kind() == token.SqlTokenKind.LIT_NUM)
            ignoring =self.cur_val()
            self.step()
            assert(self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'lines'))
            self.step()
        assert(self.cur_val() == None or self.cur_val() == ';')
        load(origin,table_name,ignoring)

    def drop(self):
        raise NotImplementedError

    def createAS(self,table_name,if_not_exists):
        raise NotImplementedError

    def select(self):
        file_name = None
        where_cond = None
        group_field = None
        group_cond = None
        order_fields = None

        fields = [self.cur_val()]
        while self.cur_kind() != token.SqlTokenKind.KEYWORD:
            fields.append(self.cur_val())
            self.step()
        assert(self.cur_kind() == token.SqlTokenKind.KEYWORD)
        if self.cur_val() == 'into':
            self.step()
            assert(self.cur_val() == 'outfile')
            self.step()
            assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
            file_name - self.cur_val() 
            self.step()
        assert(self.cur_val() == 'from')
        self.step()
        assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
        origin = self.cur_val()
        self.step()
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'where'):
            self.step()
            assert(self.cur_kind() == token.SqlTokenKind.KEYWORD)
            where_cond = self.cur_val()
            self.step()
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'group'):
            self.step()
            assert(self.cur_val() == 'by')
            self.step()
            assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
            group_field = self.cur_val()
            self.step()
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'having'):
            self.step()
            assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
            group_cond = self.cur_val()
            self.step()
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'order'):
            self.step()
            assert(self.cur_val() == 'by')
            self.step()
            assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
            order_fields = [self.cur_val()]
            self.step()
            while self.cur_kind() != token.SqlTokenKind.OPERATOR or self.tokens[self._index] != ';':
                order_fields.append(self.cur_val())
                self.step()
        assert(self.cur_val() == None or self.cur_val() == ';')
        select(fields,file_name,origin,where_cond,group_field,group_cond,order_fields)            
    
            
text1 = r""" create table 
            if not exists abc 
        (a int, 
            b varchar, 
            c float)


             ;"""       
text2 = r""" load data  
             infile 
             
             
             
             abc
             
                     into table 
                     
                        aaa
        ;"""
text3 = 'create table movies2 (title varchar,year int,duration int,score float)'
if __name__ == '__main__':
    parser(text3)
    