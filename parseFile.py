import sys

with open(sys.argv[1]) as f:
    dates = []
    for line in f:
        if line[0] == 'F':
            continue
        print(line.strip()[-5:] )
