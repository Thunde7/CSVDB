import sqltokenizer as token
from cmds import *
from Tok import Tok
from Errors import CSVDBSyntaxError


    
            
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
text3 = 'create table movies2 (title varchar,year int,duration int,score float);'

text4 = r""" drop table                                                
   movies2
        ;"""

class Parser(object):
    
    def __init__(self,text):
        self.token = token.SqlTokenizer(text)
        self.tokens = []
        self._index = 0
        while True:
            tok, val = self.token.next_token()
            line, col = self.token.cur_text_location()
            self.tokens.append(Tok(tok,val,line,col))
            if tok == token.SqlTokenKind.EOF:
                break
        print("text digested")

    def process(self):
        'getting the correct type of parsing,returns an error if not a function of SQL'
        print("Processing text")
        if self.cur_val() == 'load':
            self.step()
            return self.load()
        elif self.cur_val() == 'drop':
            self.step()
            return self.drop()
        elif self.cur_val() == 'create':
            self.step()
            return self.create()
        elif self.cur_val() == 'select':
            self.step()
            return self.select()
        else: raise CSVDBSyntaxError("unrecognized Function\n",self.tokens[self._index].line,
                                        self.tokens[self._index].col, "please check your Input!")

    def step(self,n=1):
        self._index += n

    def cur_val(self):
        return self.tokens[self._index].val
    
    def cur_kind(self):
        return self.tokens[self._index].kind

    def create(self):
        'parsing the create function syntax, asserting long to way to make sure its the correct syntax' 
        self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'table'))
        self.step()
        if_not_exists = False
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'if'):
            self.step()
            self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'not'))
            self.step()
            self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'exists'))
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
        self.tokens[self._index].is_kind("IDENTIFIER")
        f = self.cur_val()
        self.step()
        self.tokens[self._index].is_kind("KEYWORD")
        t = self.cur_val()
        fields = {f:t}
        self.step()
        while self.cur_kind() != token.SqlTokenKind.OPERATOR or self.cur_val() != ')':
            if self.cur_val() == "," : self.step()
            self.tokens[self._index].is_kind("IDENTIFIER")
            fiel = self.cur_val()
            self.step()
            self.tokens[self._index].is_kind("KEYWORD")
            typ = self.cur_val()
            fields[fiel] = typ
            self.step()
        self.step()
        assert(self.cur_val() == None or self.cur_val() == ';')
        return create(table_name,if_not_exists,fields)            
    
    def load(self):
        'parsing the load function syntax, asserting long to way to make sure its the correct syntax'
        self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'data'))
        self.step()
        self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'infile'))
        self.step()
        assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
        origin = self.cur_val()
        self.step()
        self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'into'))
        self.step()
        self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'table'))
        self.step()
        assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
        table_name = self.cur_val()
        self.step()
        ignoring = 0
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'ignore'):
            self.step()
            self.tokens[self._index].is_kind("LIT_NUM")
            ignoring =self.cur_val()
            self.step()
            self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'lines'))
            self.step()
        assert(self.cur_val() == None or self.cur_val() == ';')
        return load(origin,table_name,ignoring)


    def drop(self):
        'parsing the drop function syntax, asserting long to way to make sure its the correct syntax'
        self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'table'))
        self.step()
        if_exists = False
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'if'):
            self.step()
            self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'exists'))
            self.step()
            if_exists = True
        self.tokens[self._index].is_kind("IDENTIFIER")
        table_name = self.cur_val()
        return drop(table_name,if_exists)

    def createAS(self,table_name,if_not_exists):
        raise NotImplementedError

    def select(self):
        'parsing the select function syntax, asserting long to way to make sure its the correct syntax'
        file_name = None
        where_cond = None
        group_field = None
        group_cond = None
        order_fields = None

        fields = [self.cur_val()]
        while self.cur_kind() != token.SqlTokenKind.KEYWORD:
            fields.append(self.cur_val())
            self.step()
        self.tokens[self._index].is_kind("KEYWORD")
        if self.cur_val() == 'into':
            self.step()
            self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'outfile'))
            self.step()
            self.tokens[self._index].is_kind("IDENTIFIER")
            file_name - self.cur_val() 
            self.step()
        self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'from'))
        self.step()
        self.tokens[self._index].is_kind("IDENTIFIER")
        origin = self.cur_val()
        self.step()
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'where'):
            self.step()
            self.tokens[self._index].is_kind("IDENTIFIER")
            where_cond.append(self.cur_val()) 
            self.step()
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'group'):
            self.step()
            self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'by'))
            self.step()
            self.tokens[self._index].is_kind("IDENTIFIER")
            group_field = self.cur_val()
            self.step()
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'having'):
            self.step()
            self.tokens[self._index].is_kind("IDENTIFIER")
            group_cond = self.cur_val()
            self.step()
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'order'):
            self.step()
            self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'by'))
            self.step()
            self.tokens[self._index].is_kind("IDENTIFIER")
            order_fields = [self.cur_val()]
            self.step()
            while self.cur_kind() != token.SqlTokenKind.OPERATOR or self.tokens[self._index] != ';':
                order_fields.append(self.cur_val())
                self.step()
        assert(self.cur_val() == None or self.cur_val() == ';')
        return select(fields,file_name,origin,where_cond,group_field,group_cond,order_fields)            


