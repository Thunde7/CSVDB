class NULL(int):
    def __init__(self):
        self.num = -(2**127-1)
    
    def __repr__(self):
        return 'NULL'
    
    def __lt__(self,other):
        return self.num < other


class Columm(object):
    def __init__(self,header,typ,item_list):
        self.header = header
        self.typ = typ
        if self.typ in ['int','timestamp']:
            self.items = []
            for item in item_list:
                if item not in ["",'NULL']: self.items.append(int(item))
                else: self.items.append(NULL())
        elif self.typ == 'float': self.items = [float(item) for item in item_list]
        else: self.items = item_list
    
    def __getitem__(self,i):
        return self.items[i]

    def __repr__(self):
        return '\n'.join(self.items)

    def __len__(self):
        return len(self.items)

    def where(self,where_func):
        ##gets the condition, returns which lines answer that;
        return [line for line in range(len(self.items)) if where_func(self.items[line])]

    def get_order(self,opt):
        numbered_items = list(zip(self.items,range(len(self.items))))
        return [new_i for item,new_i in sorted(numbered_items,reverse = opt)]
    
    def order_by(self,seder):
        new_items = [self.items[i] for i in seder]
        self.items = new_items

    def group(self):
        group_lines = []
        group = []
        old_item = self.items[0] 
        for line,item in enumerate(self.items):
            if item == old_item:
                group.append(line)
            else:
                group_lines.append(group)
                group = [line]
        return group_lines

