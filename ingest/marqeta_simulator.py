from datetime import datetime, timezone, timedelta
from collections import deque
from dotenv import load_dotenv
import os, requests, time, random, json, uuid


# Load environment variables from .env file
load_dotenv()

BASE = os.getenv("MARQETA_BASE_URL")
KEY = os.getenv("MARQETA_API_KEY")
SECRET = os.getenv("MARQETA_API_SECRET")
TXN_POST_PATH = os.getenv("TXN_POST_PATH")
CARD_TOKEN = os.getenv("MARQETA_CARD_TOKEN")
MEAN_INTERVAL = float(os.getenv("SIM_INTERVAL_SEC", 5))
CURRENCY = os.getenv("CURRENCY", "USD")

# Ensure all required environment variables are set
if not all([BASE, KEY, SECRET, TXN_POST_PATH, CARD_TOKEN]):
    raise SystemExit("Missing one or more required environment variables.")

# Test connection to Marqeta API
def test_connection():
    url = f"{BASE}{TXN_POST_PATH}"
    response = requests.get(url, auth=(KEY, SECRET))
    if response.status_code != 200:
        raise SystemExit(f"Failed to connect to Marqeta API: {response.status_code} - {response.text}")
    else:
        data = response.json()
        print(data)

# Poisson distribution for number of transactions per interval (lambda=1)
def poisson_sleep(mean_interval=MEAN_INTERVAL):
    lam = 1 / max(mean_interval, 0.001) # avoid division by zero
    return random.expovariate(lam)

########## Generating random transaction data ##########
# Constants for transaction simulation
# Sample MCC codes for transaction simulation
MCC_CODES = [
    4112,  # Passenger Railways
    4119,  # Ambulance Services
    4121,  # Taxicabs and Limousines
    4131,  # Bus Lines
    4411,  # Cruise Lines
    4457,  # Boat Rentals and Leases
    4511,  # Airlines, Air Carriers
    4582,  # Airports, Flying Fields
    4722,  # Travel Agencies and Tour Operators
    4789,  # Transportation Services, Not Elsewhere Classified
    4812,  # Telecommunication Equipment and Telephone Sales
    4814,  # Telecommunication Services
    4816,  # Computer Network/Information Services
    4819,  # Cable and Other Pay Television Services
    4829,  # Cable, Satellite, and Other Pay Television and Radio Services
    4900,  # Utilities - Electric, Gas, Water, Sanitary
    5013,  # Motor Vehicle Supplies and New Parts
    5021,  # Office and Commercial Furniture
    5039,  # Construction Materials, Not Elsewhere Classified
    5045,  # Computers, Computer Peripheral Equipment, Software
    5094,  # Precious Stones, Metals, Jewelry
    5099,  # Durable Goods, Not Elsewhere Classified
    5111,  # Stationery, Office Supplies, Printing and Writing Paper
    5122,  # Drugs, Drug Proprietaries, and Druggist Sundries
    5131,  # Piece Goods, Notions, and Other Dry Goods
    5411,  # Grocery Stores, Supermarkets
    5592,  # Motor Vehicle Supplies and New Parts
    5712,  # Furniture, Home Furnishings, and Equipment
    5812,  # Eating Places, Restaurants
    5814,  # Fast Food Restaurants
    5912,  # Drug Stores and Pharmacies
    5933,  # Used Merchandise and Secondhand Stores
    5942,  # Book Stores, Amazon.com
    5960,  # Direct Marketing - Insurance Services
    5962,  # Direct Marketing - Travel Related Arrangements
    5963,  # Direct Marketing - Catalog Merchants
    5964,  # Direct Marketing - Continuity/Subscription Merchants
    5965,  # Direct Marketing - Combination Catalog and Retail Merchants
    5966,  # Direct Marketing - Outbound Telemarketing Merchants
    5967,  # Direct Marketing - Inbound Telemarketing Merchants
    5968,  # Direct Marketing - Continuity/Subscription Merchants
    5969,  # Direct Marketing - Other Direct Marketers
    5970,  # Artist's Supply and Craft Shops
    5972,  # Art Dealers and Galleries
    5993,  # Cigar Stores and Stands
    5999,  # Miscellaneous and Specialty Retail Stores
    6011,  # Automated Cash Disbursements
    6012,  # Financial Institutions - Merchandise and Services
    6051,  # Non-Financial Institutions - Foreign Currency, Money Orders (not wire transfer), and Travelers Cheques
    6211,  # Security Brokers/Dealers
    6300,  # Insurance Sales, Underwriting, and Premiums
    6540,  # Stored Value Card Purchase/Load
    7011,  # Lodging - Hotels, Motels, Resorts
    7012,  # Timeshares
    7032,  # Sporting and Recreational Camps
    7210,  # Laundry, Cleaning Services
    7230,  # Beauty and Barber Shops
    7273,  # Dating and Escort Services
    7276,  # Tax Preparation Services
    7277,  # Counseling Services - Debt, Marriage, Personal
    7299,  # Miscellaneous Personal Services
    7311,  # Advertising Services
    7321,  # Consumer Credit Reporting Agencies
    7333,  # Commercial Photography
    7342,  # Exterminating Services
    7352,  # Equipment Rental and Leasing
    7361,  # Employment Agencies
    7372,  # Computer Programming Services
    7399,  # Business Services (Not Elsewhere Classified)
    7519,  # Automotive Rental and Leasing
    7523,  # Parking Lots and Garages
    7531,  # Automotive Body Repair Shops
    7841,  # Video Tape Rental Stores
    7911,  # Dance Halls, Studios, and Schools
    7922,  # Theatrical Producers (Except Motion Pictures)
    7929,  # Bands, Orchestras, Entertainers (Not Elsewhere Classified)
    7933,  # Bowling Alleys
    7991,  # Tourist Attractions and Exhibits
    7993,  # Coin-Operated Devices (Not Elsewhere Classified)
    7994,  # Video Game Arcades/Establishments
    7995,  # Gambling Transactions
    7996,  # Amusement Parks, Carnivals, Circuses, Fortune Tellers
    7997,  # Clubs (Card, Social, Athletic)
    7998,  # Aquariums, Seaquariums, Dolphinariums
    7999,  # Recreation Services (Not Elsewhere Classified)
    8221,  # Educational Services (Not Elsewhere Classified)
    8241,  # Correspondence Schools
    8244,  # Business and Secretarial Schools
    8249,  # Vocational and Trade Schools
    9223,  # Bail and Bond Payments
    9311,  # Tax Payments
    9399,  # Government Services (Not Elsewhere Classified)
    9402,  # Postal Services - Government Only
]

# High-risk MCC codes for transaction simulation. From High-Risk Merchant Category Codes (https://www.payprin.com/resources/high-risk-merchant-category-codes-mcc)
HIGH_RISK_MCC = {
    "4112", # Passenger railways
    "4121", # Taxicabs & limousines
    "4131", # Bus lines
    "4411", # Cruise lines
    "4511", # Airlines, air carriers
    "4722", # Travel agencies & tour operators
    "4812", # Telecommunication equipment & telephone sales
    "4814", # Telecommunication services
    "4816", # Computer network/information services
    "4829", # Money transfer
    "5094", # Precious stones, metals, jewelry
    "5122", # Drugs & sundries
    "5592", # Motor vehicle supplies & new parts
    "5712", # Furniture, home furnishings, & equipment
    "5912", # Drug stores & pharmacies
    "5933", # Pawn shops
    "5960", "5962", "5963", "5964", "5965", "5966", "5967", "5968", "5969", # Direct marketing categories
    "5972", # Art dealers & galleries
    "5993", # Tobacco/vape
    "5999", # Firearms & specialty retail
    "6051", # Quasi-cash, money orders
    "6211", # Securities dealers
    "6540", # Stored value / prepaid load
    "7012", # Timeshares
    "7273", "7277", # Dating / counseling services
    "7361", # Employment agencies
    "7519", # Automotive rental/leasing
    "7841", # Video tape rental
    "7922", # Theatrical producers
    "7993", # Coin-operated devices
    "7994", "7997", # Gaming, clubs
    "9223", # Bail/bond payments
    "9399", # Government services (catch-all)
}

# Sample country codes for transaction simulation
COUNTRIES_CODES = [
    "US", "CA", "GB", "FR", "DE", "IT", "ES", "AU", "JP", "CN", "IN", "BR", "MX", "RU", "ZA", "NG", "EG", "TR", "SA", "AE", "KR"
]

# Sample channels for transaction simulation
CHANNELS = [
    "ecomm", # e-commerce
    "pos", # point of sale
    "moto", # mail order / telephone order
    "atm", # automated teller machine
    "ivr", # interactive voice response
    "other", # other
    "inapp" # in-app
    ]

# Function to get the current time in ISO 8601 format with timezone info. For example: '2023-10-05T14:48:00+00:00'.
def generate_present_iso():
    return datetime.now(timezone.utc).isoformat()

# Function to generate a random transaction amount
def generate_amount():
    return round(random.uniform(1.00, 9999.99), 2)

# Function to generate a random merchant
def generate_merchant():
    return  {
        "mid": f"M{random.randint(100000, 999999)}",
        "mcc": random.choice(MCC_CODES),
    }

# Function to generate a random geolocation
def generate_geolocation():
    lat = round(random.uniform(-60.00,60.00), 6)
    lon = round(random.uniform(-180.00,180.00), 6)
    return lat, lon

def assign_true_fraud(txn, last_txn=None, recent_same_card_count: int = 1) -> dict:
    """
    Rule-based truth label injection.
    Returns txn with event_metadata.truth_fraud (0/1) and truth_score_prob (float).
    Rules:
      - Baseline 2%
      - High-risk MCC +10%~20%
      - Cross-border +15% (if last_txn country exists and differs)
      - Night-time UTC (23-06) +5%
      - Same-card burst (>=4 within simple count) +25%
      - High-amount (>=5000) +10%
    The probabilities are additive, capped at 95%.
    Randomness is introduced in high-risk MCC adjustment.
    """

    base_fraud_prob = 0.02

    try:
        mcc = str(txn['event_metadata']['merchant']['mcc'])
    except Exception:
        mcc = ""

    # High-risk MCC
    if mcc in HIGH_RISK_MCC:
        base_fraud_prob += random.uniform(0.10, 0.20)

    # Cross-border transaction
    if last_txn:
        last_tx_country = last_txn.get('event_metadata', {}).get('country')
        curr_tx_country = txn.get('event_metadata', {}).get('country')
        if last_tx_country and curr_tx_country and last_tx_country != curr_tx_country:
            base_fraud_prob += 0.15

    # Night-time transaction (UTC 23:00 - 06:00)
    try:
        tx_time = datetime.fromisoformat(txn['user_transaction_time'])
        if tx_time.hour >= 23 or tx_time.hour < 6:
            base_fraud_prob += 0.05
    except Exception:
        pass

    # Same-card burst (>=4 transactions within simple count)
    if recent_same_card_count >= 4:
        base_fraud_prob += 0.25

    # High-amount transaction (>=5000)
    try:
        if txn['amount'] >= 5000:
            base_fraud_prob += 0.10
    except Exception:
        pass

    # Cap the probability at 99%. Return the minimum of base_fraud_prob and 0.95
    base_fraud_prob = min(base_fraud_prob, 0.95)

    # Assign truth label based on probability
    is_fraud = 1 if base_fraud_prob > random.random() else 0 # random.random() gives [0.0, 1.0]
    txn.setdefault('event_metadata', {})["truth_fraud"] = is_fraud # setdefault to ensure event_metadata exists
    txn['event_metadata']["truth_score_prob"] = round(base_fraud_prob, 4) # rounded to 4 decimal places

    return txn


# Function to build the transaction payload
def build_payload():
    tx_time = generate_present_iso()
    tx_amount = generate_amount()
    tx_merchant = generate_merchant()
    tx_lat, tx_lon = generate_geolocation()
    tx_country = random.choice(COUNTRIES_CODES)
    tx_channel = random.choice(CHANNELS)

    payload = {
        "type": "authorization",
        "card_token": CARD_TOKEN,
        "card_acceptor": { "mid": "123456890" },
        "network": "VISA",
        "amount": tx_amount,
        "currency": CURRENCY,
        "user_transaction_time": tx_time,
        "event_metadata": {
            "network": "visa",
            "merchant": tx_merchant,
            "country": tx_country,
            "channel": tx_channel,
            "device_id": str(uuid.uuid4()),
            "lat": tx_lat,
            "lon": tx_lon
        }
    }

    return payload

# Post the transaction to Marqeta API
def post_transaction(session, payload: dict) -> dict:
    url = f"{BASE}{TXN_POST_PATH}"
    headers = {
        "Content-Type": "application/json"
    }
    response = session.post(url, auth=(KEY, SECRET), headers=headers, data=json.dumps(payload))
    if response.status_code == 200 or response.status_code == 201:
        return response.json()
    else:
        print(f"Error posting transaction: {response.status_code} - {response.text}")
        return None

# Same-card rapid transactions tracking state, per-card deque of recent timestamps
recent_ts = {}  # dict: card_token -> deque[datetime]
WINDOW_SEC = 300       # 5 minutes window
IDLE_RESET_SEC = 60    # exceeding this idle time resets the deque

# Track recent transaction counts per card_token
def track_recent_counts(card_token: str, ts_iso: str) -> int:
    """
    Update the timestamp deque for a card and return the count within the window.
    - If the gap from the last timestamp exceeds IDLE_RESET_SEC, clear the deque (to avoid slow accumulation false positives)
    - Remove old timestamps outside the window
    """
    now_dt = datetime.fromisoformat(ts_iso)
    dq = recent_ts.setdefault(card_token, deque())

    if dq and (now_dt - dq[-1]).total_seconds() > IDLE_RESET_SEC:
        dq.clear()

    dq.append(now_dt)

    cutoff = now_dt - timedelta(seconds=WINDOW_SEC)
    while dq and dq[0] < cutoff:
        dq.popleft()

    if not dq:
        recent_ts.pop(card_token, None)

    return len(dq)

# Example usage for NOW
def main():
    print(f"Posting simulated transactions to Marqeta API - {BASE}{TXN_POST_PATH}")
    last_txn = None
    with requests.Session() as session:
        # test_connection() # Test connection at the start
        while True:
            try:
                payload = build_payload()
                count_in_window = track_recent_counts(payload.get('card_token'), payload['user_transaction_time'])
                payload = assign_true_fraud(payload, last_txn, recent_same_card_count=count_in_window)
                response = post_transaction(session, payload)
                if response:
                    print(f"Posted transaction: {json.dumps(response)}")
                    last_txn = payload

            except Exception as e:
                print(f"Error: {e}")

            time.sleep(poisson_sleep(MEAN_INTERVAL))


if __name__ == "__main__":
    main()