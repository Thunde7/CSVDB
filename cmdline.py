#################
#CSVDB Comandline
#################
import DBparser, os, argparse

##############
# Argparser
##############
ap = argparse.ArgumentParser()
ap.add_argument("--rootdir",help = "directory under which all tables are stored",default=".")
ap.add_argument("--run", help = "run all commands in a certain file")
ap.add_argument("--verbose", help = "print log messages for debugging",action = "store_true")
args = ap.parse_args()
##############
# Preperation
##############
os.chdir(args.rootdir)

if args.run:
    with open(args.run) as cmdfile:
        cmd_list = cmdfile.read().split(";")
else:
    cmd_list = get_cmd_text().split(";")

try:
    for cmd in cmd_list:
        Node = DBparser.Parser(cmd+';').process()
        Node.execute(args.verbose)
except Exception as e:
        print("the Exception was {} {}".format(e,type(e)))

def get_cmd_text():
    while True:
        cmd = input('csvdb> ').strip()
        while cmd != '' and cmd[-1] != ';': #need to make comments work
            inp = input().strip()
            if "--" in inp:
                inp = inp[:inp.index("--")]
            cmd += inp
        return cmd

