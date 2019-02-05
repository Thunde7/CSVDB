class Table(object):
    def __init__(self,columms,length = None):
        self.columms = {col.header : col for col in columms}
        self.length = length if length is not None else len(columms[0])
        self.rows = [[str(col[i]) for col in columms] for i in range(self.length)]

    def __repr__(self):
        le = min(self.length,100)
        return '\n'.join([','.join(row) for row in self.rows[:le]])

    def __iter__(self):
        self.current = 0
        return self

    def __next__(self):
        if self.current == self.length: raise StopIteration
        self.current += 1
        return self.rows[self.current-1]

    def order(self,exprLst):
        for field,opt in exprLst:
            order = self.columms[field].get_order(opt == 'desc') # false if asc, true if desc
            for col in self.columms.values():
                col.order_by(order)
        self.rows = [[str(col[i]) for col in self.columms.values()] for i in range(self.length)]
