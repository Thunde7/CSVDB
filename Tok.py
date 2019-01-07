################
#Token digesting
################
class Tok(object):
    def __init__(self,kind,val,line=None,col=None):
        self.kind = kind
        self.val = val
        self.line = line
        self.col = col

    def __repr__(self):
        return "{}:{}".format(self.kind,self.val)

    def __eq__(self,other):
        return self.kind == other.kind and self.val == other.val

    def is_equal(self,other):
        try:
            assert(self == other)
        except AssertionError:
            raise AssertionError("error at line {}, columm {}, should be equal to {}".format(self.line,self.col,other))