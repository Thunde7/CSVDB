################
#Token digesting
################
from sqltokenizer import *
class Tok(object):
    def __init__(self,kind,val,line=None,col=None):
        self.kind = kind
        self.val = val
        self.line = line
        self.col = col

    def __str__(self):
        return "{}:{}".format(self.kind,self.val)

    def __hash__(self):
        return 31 * (self.col - self.line) 

    def __eq__(self,other):
        return self.kind == other.kind and self.val == other.val
        
    def is_equal(self,other):
        try:
            assert(self == other)
            return True
        except AssertionError:
            raise AssertionError("error at line {}, column {}, should be equal to {}\n was {}".format(self.line,self.col,other,self))

    def is_kind(self,kinds):
        doesfail = True
        for kind in kinds:
            if (self.kind == eval("SqlTokenKind.{}".format(kind))):
                doesfail = False
        
        if doesfail:
            raise AssertionError("error at line {}, column {}, should be of {} kind".format(self.line,self.col,kind))
    
   