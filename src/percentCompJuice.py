import sys
import collections
import re

def percent_juice():
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    with open(input_file, 'r') as input, open(output_file, 'w') as output:
        # Create the file if it doesn't exist
        print("Creating file " + output_file)
        mp = collections.defaultdict(int)
        count = 0
        for line in input:
            # Split the line by tab character
            parts = line.strip().split("\t")
            # find value of "Detection_"
            x = re.search(r'"Detection_": "(.*?)"', parts[1]).group(1)
            print(x)
            if len(parts) >= 2:
                count += 1
                mp[x] += 1
        for k in mp:
            output.write(k + "\t" + str(mp[k] / count) + "\n")

if __name__ == "__main__":
    percent_juice()