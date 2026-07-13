"""Constants for the Meridian Energy integration."""

from datetime import timedelta

DOMAIN = "meridian_energy"
NAME = "Meridian Energy"

CONF_REFRESH_TOKEN = "refresh_token"
CONF_FIREBASE_USER_ID = "firebase_user_id"

AUTH_EMAIL_URL = "https://auth.meridianenergy.nz/cf/email-connector"
AUTH_OTP_URL = "https://auth.meridianenergy.nz/cf/email-otp-authenticator"
FIREBASE_API_KEY = "AIzaSyCYCKXQhGmo7haJxAAyO_7mIPrV7jtxsK8"
FIREBASE_CUSTOM_TOKEN_URL = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken"
)
FIREBASE_REFRESH_URL = "https://securetoken.googleapis.com/v1/token"
GRAPHQL_URL = "https://api.meridianenergy.nz/v1/graphql/"
REDIRECT_URL = "https://app.meridianenergy.nz?srcPlatformOS=web"

BRAND = "meridian"
CLIENT_PLATFORM = "web"
REQUEST_TIMEOUT_SECONDS = 30

UPDATE_INTERVAL = timedelta(hours=3)
INITIAL_BACKFILL = timedelta(days=365)
REVISION_OVERLAP = timedelta(days=14)
PAGE_SIZE = 744

STAT_CONSUMPTION = "consumption"
STAT_CONSUMPTION_COST = "consumption_cost"
STAT_GENERATION = "generation"
STAT_GENERATION_CREDIT = "generation_credit"

READING_CONSUMPTION = "CONSUMPTION"
READING_GENERATION = "GENERATION"
READING_FREQUENCY_HOUR = "HOUR_INTERVAL"
READING_QUALITY_COMBINED = "COMBINED"

COST_STATISTIC_TYPES = frozenset({"CONSUMPTION_COST", "STANDING_CHARGE_COST"})
GENERATION_CREDIT_TYPES = frozenset({"GENERATION_VALUE"})

DATA_CLIENT = "client"
DATA_COORDINATOR = "coordinator"
