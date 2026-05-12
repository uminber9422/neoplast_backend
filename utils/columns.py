"""Flexible column-name mapping for incoming CSV/XLSX files.

PRD §9 risk: "CSV files have wildly different column names". This module
provides fuzzy matching from common header variants to our canonical schema.
"""

from __future__ import annotations

import re

CANONICAL_FIELDS = (
    "name",
    "email",
    "phone",
    "company_name",
    "address",
    "city",
    "state",
    "pincode",
    "country",
    "website_csv",
    "notes",
    "fax",
)

# Map common synonyms → canonical name. Lowercase, alnum-only keys.
SYNONYMS: dict[str, str] = {
    # name
    "name": "name",
    "fullname": "name",
    "fullName": "name",
    "contactname": "name",
    "contactperson": "name",
    "contactpersonname": "name",
    "contact": "name",
    "person": "name",
    "personname": "name",
    "firstname": "name",  # we'll concatenate first+last in normalize step
    "lastname": "name",
    "owner": "name",
    "ownername": "name",
    "decisionmaker": "name",
    # email
    "email": "email",
    "emailaddress": "email",
    "emailid": "email",
    "mail": "email",
    "primaryemail": "email",
    "workemail": "email",
    "emailprimary": "email",
    # phone
    "phone": "phone",
    "phonenumber": "phone",
    "phoneno": "phone",
    "mobile": "phone",
    "mobileno": "phone",
    "mobilenumber": "phone",
    "contactnumber": "phone",
    "contactno": "phone",
    "tel": "phone",
    "telephone": "phone",
    "whatsapp": "phone",
    # company
    "company": "company_name",
    "companyname": "company_name",
    "organization": "company_name",
    "organisation": "company_name",
    "businessname": "company_name",
    "firm": "company_name",
    "firmname": "company_name",
    "accountname": "company_name",
    # address
    "address": "address",
    "addressline": "address",
    "addr": "address",
    "fulladdress": "address",
    "streetaddress": "address",
    "location": "address",
    # city
    "city": "city",
    "town": "city",
    # state
    "state": "state",
    "province": "state",
    "region": "state",
    # pincode
    "pincode": "pincode",
    "pin": "pincode",
    "zipcode": "pincode",
    "zip": "pincode",
    "postalcode": "pincode",
    "postcode": "pincode",
    # country
    "country": "country",
    "nation": "country",
    "countryname": "country",
    "countrycode": "country",
    # website (from CSV — kept separate from enrichment-derived company_website)
    "website": "website_csv",
    "websiteurl": "website_csv",
    "url": "website_csv",
    "weburl": "website_csv",
    "homepage": "website_csv",
    "site": "website_csv",
    "companywebsite": "website_csv",
    "companyurl": "website_csv",
    "domain": "website_csv",
    # notes / remarks
    "notes": "notes",
    "note": "notes",
    "remarks": "notes",
    "remark": "notes",
    "comments": "notes",
    "comment": "notes",
    "description": "notes",
    "details": "notes",
    "info": "notes",
    "additionalinfo": "notes",
    # fax
    "fax": "fax",
    "faxnumber": "fax",
    "faxno": "fax",
}

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _normalize_header(header: str) -> str:
    return _NON_ALNUM.sub("", header.lower())


def map_columns(headers: list[str]) -> dict[str, str]:
    """Return mapping {original_header: canonical_field} for matched columns.

    Headers without a known synonym are omitted; their data is still preserved
    in `raw_data` (JSON) by the ingest step.
    """
    mapping: dict[str, str] = {}
    for h in headers:
        key = _normalize_header(h)
        if key in SYNONYMS:
            mapping[h] = SYNONYMS[key]
    return mapping
