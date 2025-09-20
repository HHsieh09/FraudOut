from dotenv import load_dotenv
import os, requests

# Load environment variables from .env file
load_dotenv()

BASE = os.getenv("MARQETA_BASE_URL")
KEY = os.getenv("MARQETA_API_KEY")
SECRET = os.getenv("MARQETA_API_SECRET")
TXN_POST_PATH = os.getenv("TXN_POST_PATH")
CARD_TOKEN = os.getenv("MARQETA_CARD_TOKEN")
INTERVAL = int(os.getenv("SIM_INTEERVAL_SEC", 10))
CURRENCY = os.getenv("CURRENCY", "USD")

# Ensure all required environment variables are set
if not all([BASE, KEY, SECRET, TXN_POST_PATH, CARD_TOKEN]):
    raise SystemExit("Missing one or more required environment variables.")

# Test connection to Marqeta API
def test_connection():
    url = f"{BASE}{TXN_POST_PATH}"
    response = requests.get(url, auth=(KEY, SECRET))
    data = response.json()
    print(data)

# Example usage for NOW
def main():
    url = f"{BASE}{TXN_POST_PATH}"
    response = requests.get(url, auth=(KEY, SECRET))
    data = response.json()
    print(data)

if __name__ == "__main__":
    main()
    