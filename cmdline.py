#################
#CSVDB Comandline
#################
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
    except Exception as e:
        print("the Exception was: {} {}".format(e,type(e)))

            
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
text3 = 'create table aab (title varchar,year int,duration int,score float);'

text4 = r""" drop table                                                
   aaa
        ;"""

