"""GraphQL documents and privacy-safe response error helpers."""

from __future__ import annotations

import re
from typing import Any

AUTH_GRAPHQL_CODES = frozenset(
    {"KT-CT-1111", "KT-CT-1112", "KT-CT-1120", "KT-CT-1124", "KT-CT-1143"}
)
_ERROR_CODE_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,64}")
# What an error without a stable code is named, a failure inside the service say.
UNKNOWN_ERROR_CODE = "UNKNOWN"


def graphql_error_code(error: dict[str, Any]) -> str:
    """Return only the stable upstream error code, never its message."""
    extensions = error.get("extensions")
    if not isinstance(extensions, dict):
        return UNKNOWN_ERROR_CODE
    code = extensions.get("errorCode")
    if not isinstance(code, str) or _ERROR_CODE_PATTERN.fullmatch(code) is None:
        return UNKNOWN_ERROR_CODE
    return code


ACCOUNTS_QUERY = """
query accountsList($allowedBrandCodes: [BrandChoices]) {
  viewer {
    accounts(allowedBrandCodes: $allowedBrandCodes) {
      number
      status
      ... on AccountType { id }
    }
  }
}
"""

ACCOUNT_QUERY = """
query account($accountNumber: String!, $activeFrom: DateTime) {
  account(accountNumber: $accountNumber) {
    number
    status
    properties(activeFrom: $activeFrom) {
      id
      address
      meterPoints {
        id
        marketIdentifier
        registers { identifier activeFrom activeTo isFeedIn }
      }
    }
  }
}
"""

# Meridian's web app names its usage query GetMeasurements, and sends it to the
# GraphQL URL with ``?opName=GetMeasurements`` added. The integration does the
# same. The name and the URL change together, never one without the other:
# set this to False to send the integration's earlier name, measurements, to
# the bare URL again, should a live check find the web app's form refused.
MEASUREMENTS_WEB_APP_NAME = True

# Kraken's in-band limits, which arrive in an HTTP 200 body: a rate limit, and
# a query whose complexity or node count is over the limit.
RATE_LIMIT_CODE = "KT-CT-1199"
QUERY_LIMIT_CODES = frozenset({"KT-CT-1188", "KT-CT-1189"})


def measurements_operation(
    graphql_url: str, *, web_app_name: bool = MEASUREMENTS_WEB_APP_NAME
) -> tuple[str, str]:
    """Return the measurements query's operation name and the URL it is sent to."""
    if not web_app_name:
        return "measurements", graphql_url
    name = "GetMeasurements"
    return name, f"{graphql_url}?opName={name}"


def measurements_query(operation_name: str) -> str:
    """
    Return the measurements query document, named as it is sent.

    Both bounds and the page size are non-null variables, so a request that
    left one out would fail validation instead of asking for the whole
    history. A continuation page adds ``before``.
    """
    return f"""
fragment MeasurementFields on MeasurementConnection {{
  pageInfo {{ hasNextPage hasPreviousPage startCursor endCursor }}
  edges {{
    node {{
      source
      value
      unit
      readAt
      ... on IntervalMeasurementType {{ startAt endAt }}
      metaData {{
        utilityFilters {{
          ... on ElectricityFiltersOutput {{
            readingFrequencyType
            readingDirection
            registerId
            deviceId
            marketSupplyPointId
            readingQuality
          }}
        }}
        statistics {{ type costInclTax {{ estimatedAmount }} }}
      }}
    }}
  }}
}}
query {operation_name}(
  $accountNumber: String!
  $propertyId: ID!
  $before: String
  $last: Int!
  $startOn: Date!
  $endOn: Date!
  $readingFrequencyType: ReadingFrequencyType!
  $readingDirectionType: ReadingDirectionType
  $readingQualityType: ReadingQualityType
) {{
  account(accountNumber: $accountNumber) {{
    id
    property(id: $propertyId) {{
      id
      measurements(
        before: $before
        last: $last
        startOn: $startOn
        endOn: $endOn
        timezone: "Pacific/Auckland"
        utilityFilters: [{{ electricityFilters: {{
          readingDirection: $readingDirectionType
          readingQuality: $readingQualityType
          readingFrequencyType: $readingFrequencyType
        }}}}]
      ) {{ ... on MeasurementConnection {{ ...MeasurementFields }} }}
    }}
  }}
}}
"""


BILLING_PERIODS_QUERY = """
query billingPeriods($accountNumber: String!) {
  account(accountNumber: $accountNumber) {
    billingOptions {
      periodLength
      periodLengthMultiplier
      isFixed
      currentBillingPeriodStartDate
      currentBillingPeriodEndDate
      nextBillingDate
      periodStartDay
    }
  }
}
"""
