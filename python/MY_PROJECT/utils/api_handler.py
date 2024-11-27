from dotenv import load_dotenv
import os
import tradingeconomics as te
load_dotenv()

# Handling API
class TradingEconomicsLoad:
    def __init__(self, api_key_name): 
        api_key = os.getenv(api_key_name)
        if not api_key:
            raise ValueError(f"API key '{api_key_name}' not found in environment variables.")
        # Loging with the API_KEY 
        te.login(api_key) 
        self.api_key = api_key
        print("Successfully logged in with API key")
    #
    def get_api_key(self):
        return self.api_key

# # Example usage:
# try:
#     te_client = TradingEconomicsLoad("TE_CREDS")
# except ValueError as e:
#     print(f"Error: {e}")

