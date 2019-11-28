import csv
import sys

def main():
    if (len(sys.argv) < 2):
        print("Usage: python3 ./indexHeaders.py <input_csv>")
    else:
        with open(sys.argv[1]) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            header = next(csv_reader)
            for index in range(0, len(header)-1):
                print("%d\t%s" % (index, header[index]))

if __name__ == '__main__':
    main()
