"""
Where D1.ID == D2.Name
python3 joinJuice.py <input_file> <output_file>
python3 joinJuice.py joinMapleInterNone.txt output.txt
"""


import sys
import re


def joinJuice():
    if len(sys.argv) != 3:
        print(
            "Usage: joinJuice.py <input_file> <output_file>"
        )
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # if D1 is present and D2 is present
    # output everything
    # if not, output empty file

    with open(input_file, "r") as f:
        lines = f.readlines()

    s = set()
    for line in lines:
        _, val = line.strip().split("\t")
        D = val.split(",")[0]
        s.add(D)

    if len(s) == 2:
        with open(output_file, "w") as f:
            for line in lines:
                val = line.strip().split("\t")[1]
                f.write(val[4:]+ "\n")


if __name__ == "__main__":
    joinJuice()
