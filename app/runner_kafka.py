import os, sys
from dotenv import load_dotenv
from producer_kafka import main as produce_main

load_dotenv()
def parse_n():
    if len(sys.argv) > 1:
        try: return int(sys.argv[1])
        except: pass
    return int(os.getenv("NUM_RECORDS", "50"))

if __name__ == "__main__":
    produce_main(parse_n())
