import re
import sys

def filter_maple():
    regex = sys.argv[1]
    input_file = sys.argv[2]
    output_file = sys.argv[3]

    with open(output_file, 'w+') as output:
        # Create the file if it doesn't exist
        print("Creating file " + output_file)

        with open(input_file, 'r') as input:
            for line in input:
                # Search for the regex pattern in the line
                if re.search(regex, line):
                    output.write("1\t" + line)

if __name__ == "__main__":
    filter_maple()