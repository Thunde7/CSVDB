class Table(object):
    def __init__(self,columns,length = None):
        self.columns = {col.header : col for col in columns}
        self.length = length if length is not None else len(columns[0])
        self.headers = [col.header for col in columns]
        self.rows = [self.headers] + [[str(col[i]) for col in columns] for i in range(self.length)]

    def __str__(self):
        le = min(self.length,100)
        return '\n'.join([','.join(row) for row in self.rows[:le + 1]])

    def __iter__(self):
        self.current = 0
        return self

    def __next__(self):
        if self.current == self.length: raise StopIteration
        self.current += 1
        return self.rows[self.current-1]

    def order(self,exprLst):
        for field,opt in exprLst:
            order = self.columns[field].get_order(opt == 'desc') # false if asc, true if desc
            for col in self.columns.values():
                col.order_by(order)
        self.rows = [[str(col[i]) for col in self.columns.values()] for i in range(self.length)]
