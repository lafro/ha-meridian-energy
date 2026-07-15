"""Constants for the Meridian Energy integration."""

from datetime import timedelta

DOMAIN = "meridian_energy"
NAME = "Meridian Energy"

CONF_REFRESH_TOKEN = "refresh_token"
CONF_FIREBASE_USER_ID = "firebase_user_id"
CONF_SELECTED_ACCOUNTS = "selected_accounts"
CONF_AUTO_ADD_ACCOUNTS = "auto_add_accounts"

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

UPDATE_INTERVAL = timedelta(hours=1)
INITIAL_BACKFILL = timedelta(days=90)
REVISION_OVERLAP = timedelta(days=14)
TIP_WINDOW = timedelta(hours=24)
TARGETED_RECONCILIATION_MINIMUM = timedelta(hours=48)
TARGETED_RECONCILIATION_INTERVAL = timedelta(days=1)
FULL_RECONCILIATION_INTERVAL = timedelta(days=7)
TOPOLOGY_CACHE_INTERVAL = timedelta(days=1)
BILLING_CACHE_INTERVAL = timedelta(days=1)
RECONCILIATION_SAFETY_MARGIN = timedelta(hours=6)
PAGE_SIZE = 744
MIN_PAGE_SIZE = 24
MAX_MEASUREMENT_PAGES = 256

DEFAULT_RETRY_AFTER_SECONDS = 3600
MIN_RETRY_AFTER_SECONDS = 60
MAX_RETRY_AFTER_SECONDS = 86400

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
