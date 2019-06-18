####################
#CSVDB Commandline #
####################
import DBparser, os, argparse, platform

##############
# Argparser  #
##############
ap = argparse.ArgumentParser()
ap.add_argument("--rootdir",help = "directory under which all tables are stored",default=".")
ap.add_argument("--run", help = "run all commands in a certain file")
ap.add_argument("--verbose", help = "print log messages for debugging",action = "store_true")
args = ap.parse_args()

###############
# Preperation #
###############

os.chdir(args.rootdir)
if not os.path.exists("DBS"):
    os.mkdir("DBS")
os.chdir("DBS")

def get_cmd_text():
    while True:
        cmd = input('csvdb> ').strip() + " "
        while cmd != '' and cmd[-2] != ';': #needed to make comments work
            inp = input().strip() + " "
            if "--" in inp:
                inp = inp[:inp.index("--")]
            cmd += inp
        return cmd

def clear_screen():
    if platform.system() == 'Linux':
        os.system('clear')
    elif platform.system() == 'Windows':
        os.system('cls')

def catch_exception(cmd_list,args):
    for cmd in cmd_list:
        try:
            Node = DBparser.Parser(cmd.strip()+';',args.verbose).process()
            Node.execute(args.verbose)
        except Exception as e:
            return("The parsing failed; \n The Exception was: {}; \n That exception type is :{}".format(e,type(e)))
    return(0)

def main():
    clear_screen()
    if args.run:
        with open(args.run) as cmdfile:
            cmd_list = cmdfile.read().split(";")
        err = catch_exception(cmd_list,args)
        if not err:
            print("Finished with 0 Errors encounterd")
    else:
        while True:
            cmd_list = get_cmd_text().split(";")[:1]
            e = catch_exception(cmd_list,args)
            if e:
                print(e)
            else:
                print("Finished with 0 Errors encounterd")

###############
#Testing texts#
###############

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

text5 =r"""select year,title into outfile abc from movies2 where year > 2013 order by year desc title asc;
"""

if __name__ == "__main__":
    main()