import parser

while True:
    cmd = input('csvdb> ').strip()
    while cmd[-1] != ';':
        cmd += input()
        cmd = cmd.strip()
    parser = parser.parser(cmd)


def text_from_keyboard():
    print("Enter text. Finishh with a line containin ONLY ;")
    text = ""
    while True:
        line = input("csvdb> ")
        text += line + "\n"
        if text[-2] == ";":
            return text