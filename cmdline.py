import DBparser, os

##############
# Preperation
##############
path = os.getcwd()
if not os.path.exists('DBS'): os.mkdir("DBS")
dbspath = os.path.join(path,'DBS')
os.chdir(dbspath)

while True:
    cmd = input('csvdb> ').strip()
    while cmd != '' and cmd[-1] != ';':
        cmd += input()
        cmd = cmd.strip()
    try:
        Node = DBparser.Parser(cmd).process()
        Node.execute()
    except AssertionError as e:
        print(e)

