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

    def cur_tok(self):
        x = self.tokens[self._index]
        self.step()
        return x
    def cur_val(self):
        return self.tokens[self._index].val
    
    def cur_kind(self):
        return self.tokens[self._index].kind

    def create(self):
        'parsing the create function syntax, asserting long to way to make sure its the correct syntax' 
        self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'table'))
        if_not_exists = False
        if self.cur_tok() == (Tok(token.SqlTokenKind.KEYWORD,'if')):
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'not'))
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'exists'))
            if_not_exists == True
        t = self.cur_tok()
        t.is_kind("IDENTIFIER")
        table_name = t.val
        if self.cur_tok() == (Tok(token.SqlTokenKind.KEYWORD,'as')):
            self.createAS(table_name,if_not_exists)
        assert(self.cur_val() == '(')
        self.step()
        fiel = self.cur_tok()
        fiel.is_kind("IDENTIFIER")
        typ = self.cur_tok()
        typ.is_kind("KEYWORD")
        fields = {fiel.val:typ.val}
        while not self.cur_kind() == token.SqlTokenKind.OPERATOR or self.cur_val() != ')':
            if self.cur_val() == "," : self.step()
            fiel = self.cur_tok()
            fiel.is_kind("IDENTIFIER")
            typ = self.cur_tok()
            typ.is_kind("KEYWORD")
            fields[fiel.val] = typ.val
        self.step()
        assert(self.cur_val() == None or self.cur_val() == ';')
        return create(table_name,if_not_exists,fields)            
    
    def load(self):
        'parsing the load function syntax, asserting long to way to make sure its the correct syntax'
        self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'data'))
        self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'infile'))
        origin = self.cur_tok()
        origin.is_kind("IDENTIFIER")
        origin = origin.val
        self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'into'))
        self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'table'))
        table_name = self.cur_tok()
        table_name.is_kind("IDENTIFIER")
        table_name = table_name.val
        ignoring = 0
        if self.cur_tok() == Tok(token.SqlTokenKind.KEYWORD,'ignore'):
            ignoring = self.cur_tok()
            ignoring.is_kind("LIT_NUM")
            ignoring = ignoring.val
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'lines'))
        assert(self.cur_val() == None or self.cur_val() == ';')
        return load(origin,table_name,ignoring)


    def drop(self):
        'parsing the drop function syntax, asserting long to way to make sure its the correct syntax'
        self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'table'))
        self.step()
        if_exists = False
        if self.cur_tok() == Tok(token.SqlTokenKind.KEYWORD,'if'):
            self.step()
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'exists'))
            self.step()
            if_exists = True
        self.cur_tok().is_kind("IDENTIFIER")
        table_name = self.cur_val()
        return drop(table_name,if_exists)

    def createAS(self,table_name,if_not_exists):
        raise NotImplementedError

    def select(self):
        'parsing the select function syntax, asserting long to way to make sure its the correct syntax'
        file_name = None
        where_cond = []
        group_field = None
        group_cond = None
        order_fields = {}

        fields = [self.cur_val()]
        while self.cur_kind() != token.SqlTokenKind.KEYWORD:
            fields.append(self.cur_val())
            self.step()
        x = self.cur_tok()
        x.is_kind("KEYWORD")
        if x.val == 'into':
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'outfile'))
            file_name = self.cur_tok()
            file_name.is_kind("IDENTIFIER")
            file_name = file_name.val 
        self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'from'))
        origin = self.cur_tok()
        origin.is_kind("IDENTIFIER")
        origin = origin.val
        if self.cur_tok() == Tok(token.SqlTokenKind.KEYWORD,'where'):
            tmp = self.cur_tok()
            tmp.is_kind("IDENTIFIER")
            where_cond.append(tmp.val) 
            tmp = self.cur_tok()
            tmp.is_kind("OPERATOR")
            where_cond.append(tmp.val)
            tmp = self.cur_tok()
            tmp.is_kind("LIT_NUM")
            where_cond.append(tmp.val)
        if self.cur_tok() == Tok(token.SqlTokenKind.KEYWORD,'group'):
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'by'))
            group_field = self.cur_tok()
            group_field.is_kind("IDENTIFIER")
            group_field = group_field.val
        if self.cur_tok() == Tok(token.SqlTokenKind.KEYWORD,'having'):
            group_cond = self.cur_tok()
            group_cond.is_kind("IDENTIFIER")
            group_cond = group_cond.val
        if self.cur_tok() == Tok(token.SqlTokenKind.KEYWORD,'order'):
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'by'))
            field = self.cur_tok()
            field.is_kind("IDENTIFIER")
            asc_or_desc = self.cur_tok()
            assert(asc_or_desc in [Tok(token.SqlTokenKind.KEYWORD,"ASC"),Tok(token.SqlTokenKind.KEYWORD,"DESC")])
            order_fields[field.val] = asc_or_desc.val            
            while self.cur_kind() != token.SqlTokenKind.OPERATOR or self.cur_val() != ';':
                order_fields[self.cur_tok().val] = self.cur_tok().val
        assert(self.cur_val() == None or self.cur_val() == ';')
        return select(fields,file_name,origin,where_cond,group_field,group_cond,order_fields)            


