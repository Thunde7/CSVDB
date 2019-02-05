#################
#CSVDB Comandline
#################
import DBparser, os, argparse, platform
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

def get_cmd_text():
    while True:
        cmd = input('csvdb> ').strip() + " "
        while cmd != '' and cmd[-2] != ';': #need to make comments work
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

def main():
    clear_screen()
    if args.run:
        with open(args.run) as cmdfile:
            cmd_list = cmdfile.read().split(";")
    else:
        cmd_list = get_cmd_text().split(";")[:1]

    try:
        for cmd in cmd_list:
            Node = DBparser.Parser(cmd.strip()+';').process()
            Node.execute(args.verbose)
    except Exception as e:
            print("the Exception was {} {}".format(e,type(e)))


if __name__ == "__main__":
    main()