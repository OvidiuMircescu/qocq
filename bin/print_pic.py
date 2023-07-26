import pickle

def run(path):
  with open(path, "br") as f:
    obj = pickle.load(f)
  print(obj)

def main():
  import argparse
  parser = argparse.ArgumentParser(
    description="Print the pyobj read from a file where it is picklized.")
  parser.add_argument("pyfile",
                      help="Path to the file containing picklized object.")
  args = parser.parse_args()
  run(args.pyfile)

if __name__ == '__main__':
  main()
