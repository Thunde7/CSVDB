import sqltokenizer as token #easier reading
from cmds import *
from Tok import Tok
from Errors import CSVDBSyntaxError

class Parser(object):
    
    def __init__(self,text,verbose):
        self.token = token.SqlTokenizer(text) #deviding the input to tokens
        self.tokens = []
        self._index = 0
        self.verbose = verbose
        while True:
            tok, val = self.token.next_token()          # basicly, taking all the tokens
            line, col = self.token.cur_text_location()  # and putting them in a list
            self.tokens.append(Tok(tok,val,line,col))   # ordered by chronological sense
            if tok == token.SqlTokenKind.EOF:           # break at the EOF
                break
        if self.verbose:
            print("the input was digested;")

    def process(self):
        'getting the correct type of parsing,returns an error if not a function of SQL'
        if self.verbose:
            print("The input is being proccessed")
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
        'shifting the current index n steps ahead'
        self._index += n 

    def cur_tok(self):
        'the current token in the list, also shifts the index by 1'
        tmp = self.tokens[self._index]
        self.step()
        return tmp

    def cur_val(self):
        'returns the current token"s value this does not shift the index'
        return self.tokens[self._index].val
    
    def cur_kind(self):
        'returns the current token"s kind this does not shift the index'
        return self.tokens[self._index].kind

    def create(self):
        'parsing the create function syntax, asserting long to way to make sure its the correct syntax' 
        self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'table'))
        if_not_exists = False
        t = self.cur_tok()
        if t == (Tok(token.SqlTokenKind.KEYWORD,'if')):
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'not'))
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'exists'))
            if_not_exists == True
            t = self.cur_tok()  
        t.is_kind("IDENTIFIER")
        table_name = t.val
        tmp = self.cur_tok()
        if tmp == (Tok(token.SqlTokenKind.KEYWORD,'as')):
            self.createAS(table_name,if_not_exists)
        tmp.is_equal(Tok(token.SqlTokenKind.OPERATOR,'('))
        fiel = self.cur_tok()
        fiel.is_kind("IDENTIFIER")
        typ = self.cur_tok()
        typ.is_kind("KEYWORD")
        fields = {fiel.val:typ.val}
        while not self.cur_kind() == token.SqlTokenKind.OPERATOR or self.cur_val() != ')':
            if self.cur_val() == "," : self.step()  #skipping commas 
            fiel = self.cur_tok()                   #making sure the token 
            fiel.is_kind("IDENTIFIER")              #secifies the next field
            typ = self.cur_tok()                    #making sure the token
            typ.is_kind("KEYWORD")                  #specifies the next field type
            fields[fiel.val] = typ.val              #setting the field type according to the given input
        self.step()                                 # shifting the index to skip the ')'
        assert(self.cur_val() == None or self.cur_val() == ';') #making sure that its either the end or the ;
        if self.verbose:
            print("The input was proccessed, CREATING the table;")
        return create(table_name,if_not_exists,fields)            
    
    def load(self):
        'parsing the load function syntax, asserting along to way to make sure its the correct syntax'
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
        tmp = self.cur_tok()
        if tmp == Tok(token.SqlTokenKind.KEYWORD,'ignore'):
            ignoring = self.cur_tok()
            ignoring.is_kind("LIT_NUM")
            ignoring = ignoring.val
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'lines'))
            tmp = self.cur_tok()
        assert(tmp.val == None or tmp.val == ';') #making sure that its either the end or the ;
        if self.verbose:
            print("The input was proccessed, LOADING the table;")
        return load(origin,table_name,ignoring)


    def drop(self):
        'parsing the drop function syntax, asserting long to way to make sure its the correct syntax'
        self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'table'))
        if_exists = False
        tmp = self.cur_tok()
        if tmp == Tok(token.SqlTokenKind.KEYWORD,'if'):
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'exists'))
            if_exists = True
            tmp = self.cur_tok()
        tmp.is_kind("IDENTIFIER")
        table_name = tmp.val
        assert(self.cur_val() == None or self.cur_val() == ';') #making sure that its either the end or the ;
        if self.verbose:
            print("The input was proccessed, DROPPING the table;")
        
        return drop(table_name,if_exists)

    def createAS(self,table_name,if_not_exists):
        raise NotImplementedError

    def select(self):
        'parsing the select function syntax, asserting long to way to make sure its the correct syntax'
        file_name = None
        where_cond = []
        group_field = None
        group_cond = None
        order_fields = []
        fields = [self.cur_val()]
        self.step()
        while self.cur_kind() != token.SqlTokenKind.KEYWORD:
            if self.cur_val() == ',': 
                self.step()
                continue
            fields.append(self.cur_val())
            self.step()
        tmp = self.cur_tok()
        tmp.is_kind("KEYWORD")
        if tmp.val == 'into':
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'outfile'))
            file_name = self.cur_tok()
            file_name.is_kind("IDENTIFIER")
            file_name = file_name.val
            tmp = self.cur_tok()
        tmp.is_equal(Tok(token.SqlTokenKind.KEYWORD,'from'))
        origin = self.cur_tok()
        origin.is_kind("IDENTIFIER")
        origin = origin.val
        x = self.cur_tok()
        if x == Tok(token.SqlTokenKind.KEYWORD,'where'):
            tmp = self.cur_tok()
            tmp.is_kind("IDENTIFIER")
            where_cond.append(tmp.val) 
            tmp = self.cur_tok()
            tmp.is_kind("OPERATOR")
            where_cond.append(tmp.val)
            tmp = self.cur_tok()
            tmp.is_kind("LIT_NUM")
            where_cond.append(tmp.val)
            x = self.cur_tok()
        if x == Tok(token.SqlTokenKind.KEYWORD,'group'):
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'by'))
            group_field = self.cur_tok()
            group_field.is_kind("IDENTIFIER")
            group_field = group_field.val
            x = self.cur_tok()
        if x == Tok(token.SqlTokenKind.KEYWORD,'having'):
            group_cond = self.cur_tok()
            group_cond.is_kind("IDENTIFIER")
            group_cond = group_cond.val
            x = self.cur_tok()
        if x == Tok(token.SqlTokenKind.KEYWORD,'order'):
            self.cur_tok().is_equal(Tok(token.SqlTokenKind.KEYWORD,'by'))
            while not (self.cur_val() == None or self.cur_val() == ';'):
                if self.cur_val() == "," : self.step()
                field = self.cur_tok()
                field.is_kind("IDENTIFIER")
                opt = self.cur_tok()
                assert(opt in [Tok(token.SqlTokenKind.KEYWORD,'asc'),Tok(token.SqlTokenKind.KEYWORD,'desc')])
                order_fields.append((field.val,opt.val))
        assert(self.cur_val() == None or self.cur_val() == ';') #making sure that its either the end or the ;
        if self.verbose:
            print("The input was proccessed, SELECTING the table;")
        return select(fields,file_name,origin,where_cond,group_field,group_cond,order_fields)            


