import base64
import hashlib

from src.configs import config


def api_token_gen():

    password_hash = hashlib.sha256(f'{config.MIR_PASSWORD}'.encode()).hexdigest()

    raw = f'{config.MIR_ACCOUNT}:{password_hash}'

    token = base64.b64encode(raw.encode()).decode()

    return 'Basic ' + token


api_token = api_token_gen()
headers = {
    'Authorization': api_token,
    'Accept-Language': 'en-US',
}
