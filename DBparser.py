import sqltokenizer as token
from cmds import *
from Tok import Tok
import json, os, csv, shutil


    
            
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
        print("text processed")

    def process(self):
        if self.cur_val() == 'load':
            self.step()
            return self.load()
        if self.cur_val() == 'drop':
            self.step()
            return self.drop()
        if self.cur_val() == 'create':
            self.step()
            return self.create()
        if self.cur_val() == 'select':
            self.step()
            return self.select()

            
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
        return create(table_name,if_not_exists,fields)            
    
    def load(self):
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
            assert(self.cur_kind() == token.SqlTokenKind.LIT_NUM)
            ignoring =self.cur_val()
            self.step()
            self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'lines'))
            self.step()
        assert(self.cur_val() == None or self.cur_val() == ';')
        return load(origin,table_name,ignoring)

    def drop(self):
        self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'table'))
        self.step()
        if_exists = False
        if self.tokens[self._index] == Tok(token.SqlTokenKind.KEYWORD,'if'):
            self.step()
            self.tokens[self._index].is_equal(Tok(token.SqlTokenKind.KEYWORD,'exists'))
            self.step()
            if_exists = True
        assert(self.cur_kind() == token.SqlTokenKind.IDENTIFIER)
        table_name = self.cur_val()
        return drop(table_name,if_exists)

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
        return select(fields,file_name,origin,where_cond,group_field,group_cond,order_fields)            


        
if __name__ == '__main__':
    Parser(text4)