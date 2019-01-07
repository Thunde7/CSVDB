#####################
#Table related errors
#####################
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

class TableDoesNotExistError(Exception):
    def __init__(self, name):
        self.message = 'the table {} does not exist'.format(name)

    def __str__(self):
        return self.message

class TableExistsError(Exception):
    def __init__(self,name):
        self.message = 'the table {} already exists'.format(name)

    def __str__(self):
        return self.message
