"""Generate synthetic prospect CSVs for local end-to-end testing.

Outputs 5 CSVs to `data/samples/` totalling ~500 records, with ~10% intra-file
and inter-file duplicates so dedup logic gets exercised.
"""

from __future__ import annotations

import csv
import random
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from faker import Faker  # noqa: E402

fake = Faker("en_IN")
random.seed(42)
Faker.seed(42)

# Realistic Indian states + key industrial cities
INDIAN_STATES = [
    ("Maharashtra", ["Mumbai", "Pune", "Nagpur", "Aurangabad", "Nashik"]),
    ("Gujarat", ["Ahmedabad", "Surat", "Rajkot", "Vadodara", "Bhavnagar"]),
    ("Tamil Nadu", ["Chennai", "Coimbatore", "Tiruppur", "Madurai", "Salem"]),
    ("Karnataka", ["Bengaluru", "Mysuru", "Mangaluru", "Hubballi"]),
    ("Delhi", ["Delhi", "New Delhi"]),
    ("Haryana", ["Gurugram", "Faridabad", "Panipat"]),
    ("Uttar Pradesh", ["Noida", "Ghaziabad", "Kanpur", "Lucknow"]),
    ("Telangana", ["Hyderabad", "Warangal"]),
    ("West Bengal", ["Kolkata", "Howrah"]),
    ("Rajasthan", ["Jaipur", "Jodhpur", "Udaipur"]),
]

INDUSTRY_HINTS = [
    "Plastics", "Polymers", "Packaging", "Auto Parts", "Pharma", "Chemicals",
    "Pipes & Fittings", "Textiles", "FMCG", "Electronics", "Construction",
    "Food Processing", "Agri", "Rubber", "Recycling", "Trading",
]

COMPANY_SUFFIXES = [
    "Pvt Ltd", "Industries", "Polymers", "Plastics", "Enterprises", "Corporation",
    "Manufacturing Co.", "Tech", "Trading Co.", "Industries Pvt Ltd",
]

SAMPLE_FILES = [
    ("contacts_2024_q1.csv", 120, [
        "Full Name", "Email Address", "Mobile Number", "Company", "Address",
        "City", "State", "PIN Code", "Notes",
    ]),
    ("trade_show_leads.csv", 100, [
        "Name", "Email", "Phone", "Organization", "City", "State", "Industry Note",
    ]),
    ("imported_leads.xlsx", 110, [
        "First Name", "Last Name", "Work Email", "Mobile", "Company Name",
        "City", "State", "Pincode",
    ]),
    ("partner_referrals.csv", 80, [
        "Contact", "EmailID", "WhatsApp", "Firm Name", "Location",
    ]),
    ("website_inquiries.csv", 100, [
        "fullname", "email", "phone", "businessname", "address", "city", "state",
    ]),
]


def _company_name() -> str:
    industry = random.choice(INDUSTRY_HINTS)
    base = fake.last_name() if random.random() < 0.4 else fake.company().split(" ")[0]
    return f"{base} {industry} {random.choice(COMPANY_SUFFIXES)}"


def _generate_record() -> dict:
    state, cities = random.choice(INDIAN_STATES)
    city = random.choice(cities)
    name = fake.name()
    company = _company_name()
    domain = (
        company.lower()
        .replace(" pvt ltd", "")
        .replace(" pvt", "")
        .replace(" ltd", "")
        .replace(" ", "")
        .replace(".", "")[:24]
    )
    email_local = name.lower().replace(" ", ".").replace("'", "")[:30]
    email = f"{email_local}@{domain}.in"
    phone = fake.phone_number()
    return {
        "name": name,
        "email": email,
        "phone": phone,
        "company": company,
        "city": city,
        "state": state,
        "pincode": fake.postcode(),
        "address": fake.address().replace("\n", ", "),
        "industry_hint": random.choice(INDUSTRY_HINTS),
    }


def _emit_csv_row(headers: list[str], record: dict) -> dict:
    row = {}
    for h in headers:
        key = h.lower().replace(" ", "").replace("_", "")
        if key in {"fullname", "name", "contact", "fullname"}:
            row[h] = record["name"]
        elif key == "firstname":
            row[h] = record["name"].split(" ")[0]
        elif key == "lastname":
            row[h] = " ".join(record["name"].split(" ")[1:]) or ""
        elif key in {"emailaddress", "email", "emailid", "workemail"}:
            row[h] = record["email"]
        elif key in {"mobilenumber", "phone", "mobile", "whatsapp"}:
            row[h] = record["phone"]
        elif key in {"company", "organization", "companyname", "firmname", "businessname"}:
            row[h] = record["company"]
        elif key in {"address", "fulladdress"}:
            row[h] = record["address"]
        elif key in {"city", "location"}:
            row[h] = record["city"]
        elif key == "state":
            row[h] = record["state"]
        elif key in {"pincode", "pin", "zipcode"}:
            row[h] = record["pincode"]
        elif key == "industrynote":
            row[h] = record["industry_hint"]
        elif key == "notes":
            row[h] = f"Met at {record['industry_hint']} expo"
        else:
            row[h] = ""
    return row


def main() -> int:
    out_dir = PROJECT_ROOT / "data" / "samples"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build a shared pool so cross-file duplicates are realistic.
    pool: list[dict] = [_generate_record() for _ in range(450)]

    total_written = 0
    for filename, count, headers in SAMPLE_FILES:
        records: list[dict] = []
        for _ in range(count):
            # 10% chance to reuse from pool (cross-file duplicate)
            if random.random() < 0.10 and pool:
                records.append(random.choice(pool))
            else:
                records.append(_generate_record())
        # ~3% intra-file duplicates
        for _ in range(int(count * 0.03)):
            records.append(random.choice(records))

        path = out_dir / filename
        if filename.endswith(".xlsx"):
            try:
                import pandas as pd  # local import — only needed for xlsx
            except ImportError:
                print(f"  ! WARN:pandas not installed — skipping {filename}")
                continue
            df = pd.DataFrame([_emit_csv_row(headers, r) for r in records])
            df.to_excel(path, index=False)
        else:
            with path.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=headers)
                writer.writeheader()
                for r in records:
                    writer.writerow(_emit_csv_row(headers, r))
        print(f"  [OK]{filename}: {len(records)} rows")
        total_written += len(records)

    print(f"\nDone. Wrote {total_written} rows across {len(SAMPLE_FILES)} files to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
