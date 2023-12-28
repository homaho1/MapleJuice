"""
Where D1.Iterconne == D2.Detection_
python3 joinMaple.py <schema> <input_file> <output_file> <D1/D2>

output format: 
schema's value: D1/D2, all data
***different schema's value should be in different files***
"""
import re
import sys


def joinMaple():
    D ,schema = sys.argv[1].split(".")
    input_file = sys.argv[2]
    output_file = sys.argv[3]
    hash_table = {}

    with open(input_file, "r") as f:
        lines = f.readlines()

    for line in lines:
        pattern = f"\"{schema}\": \"(.*?)\""
        print(pattern)
        x = re.search(pattern, line)
        if x:
            x = x.group(1)
        else:
            continue
        if x not in hash_table:
            hash_table[x] = [line]
        else:
            hash_table[x].append(line)

    with open(output_file, "w") as f:
        for key, lines in hash_table.items():
            for line in lines:
                f.write(key + "\t" + D + ", " + line)
    print("done")

if __name__ == "__main__":
    joinMaple()
