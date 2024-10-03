import argparse

def main():
    # Create the parser
    parser = argparse.ArgumentParser(description="Display command line arguments")

    # Add arguments
    parser.add_argument('args', nargs='*', help='List of command line arguments')

    # Parse the command line arguments
    parsed_args = parser.parse_args()

    # Display the arguments
    print("Command line arguments received:")
    for i, arg in enumerate(parsed_args.args):
        print(f"Argument {i + 1}: {arg}")

if __name__ == "__main__":
    main()
