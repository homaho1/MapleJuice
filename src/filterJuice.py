import sys

def filter_juice():
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    with open(input_file, 'r') as input, open(output_file, 'w') as output:
        # Create the file if it doesn't exist
        print("Creating file " + output_file)

        for line in input:
            # Split the line by tab character
            parts = line.strip().split("\t")
            if len(parts) >= 2:
                key, value = parts[0], parts[1]
                output.write(value + "\n")

if __name__ == "__main__":
    filter_juice()