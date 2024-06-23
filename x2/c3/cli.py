import sys
def execute(args):
    """
    >>> execute([""])
    Hello World!!!
    """
    print("Hello World!!!")

def main(): execute(sys.argv[1:]) # pragma: no cover
