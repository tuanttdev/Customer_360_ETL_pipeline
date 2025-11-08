from google import genai
from google.genai import errors
import random
import time
import os
from dotenv import load_dotenv

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
API_CALL_TIMEOUT_SECONDS=120
load_dotenv()

MAX_RETRIES = 3


def call_gemini_api_with_retry(prompt, max_retries=MAX_RETRIES):
    retries = 0

    if GEMINI_API_KEY:
        client = genai.Client(api_key=GEMINI_API_KEY, http_options={ 'timeout': int(API_CALL_TIMEOUT_SECONDS) * 1000})
    else:
        print('GEMINI_API_KEY is invalid')
        return None

    while retries < max_retries:
        try:
            # Replace with your actual Gemini API call
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt
            )
            return response
        except errors.APIError as e:
            if e.code == 429:
                wait_time = (5 ** retries) + random.uniform(0, 1)  # Exponential backoff with jitter
                print(f"429 error received. Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                retries += 1
            else:
                raise  # Re-raise other ClientErrors
    print("Max retries exceeded for Gemini API call.")
    return None
