"""Thai-specific synthetic identifiers.

Everything here is fabricated. The national IDs are structurally valid (they
satisfy the real mod-11 checksum) so that downstream validation logic can be
exercised honestly, but they are drawn from a seeded PRNG and correspond to no
real person.
"""

from __future__ import annotations

import random

# Thai consonants used on licence plates (the full set is larger; this is the
# common subset that appears on private vehicle plates).
PLATE_CONSONANTS = "กขคงจฉชญฎฏฐฑฒณดตถทธนบปผฝพฟภมยรลวศษสหฬอฮ"

PROVINCES_TH = [
    "กรุงเทพมหานคร",
    "เชียงใหม่",
    "ขอนแก่น",
    "ชลบุรี",
    "ภูเก็ต",
    "นครราชสีมา",
    "สงขลา",
    "อุดรธานี",
    "สุราษฎร์ธานี",
    "ระยอง",
]
PROVINCES_EN = [
    "Bangkok",
    "Chiang Mai",
    "Khon Kaen",
    "Chonburi",
    "Phuket",
    "Nakhon Ratchasima",
    "Songkhla",
    "Udon Thani",
    "Surat Thani",
    "Rayong",
]

THAI_GIVEN_NAMES = [
    "สมชาย",
    "สมหญิง",
    "ปรีชา",
    "วิชัย",
    "นภา",
    "อรุณี",
    "ธนกร",
    "ศิริพร",
    "ประเสริฐ",
    "กมลวรรณ",
    "ชัยวัฒน์",
    "พิมพ์ใจ",
]
THAI_FAMILY_NAMES = [
    "ศรีสุข",
    "ใจดี",
    "รักไทย",
    "บุญมี",
    "วงศ์สกุล",
    "แสงทอง",
    "พรหมมา",
    "จันทร์เพ็ญ",
    "ทองดี",
    "สุวรรณ",
]

PRE_EXISTING_CONDITIONS = [
    "hypertension",
    "type_2_diabetes",
    "asthma",
    "hyperlipidaemia",
    "allergic_rhinitis",
    "migraine",
    "gastritis",
    "none",
]

BMI_BANDS = ["underweight", "normal", "overweight", "obese_i", "obese_ii"]


def national_id_check_digit(first12: str) -> int:
    """Return the mod-11 check digit for the first 12 digits of a Thai ID.

    The published algorithm: multiply digit i (0-indexed) by (13 - i), sum,
    take the remainder mod 11, subtract from 11, then take mod 10.
    """
    total = sum(int(d) * (13 - i) for i, d in enumerate(first12))
    return (11 - (total % 11)) % 10


def make_national_id(rng: random.Random, valid: bool = True) -> str:
    """Generate a 13-digit Thai national ID.

    The leading digit is constrained to 1-8 as it is in reality (it encodes the
    category of registration). When ``valid`` is False the check digit is
    deliberately corrupted so that validation tests have something to catch.
    """
    first12 = str(rng.randint(1, 8)) + "".join(str(rng.randint(0, 9)) for _ in range(11))
    check = national_id_check_digit(first12)
    if not valid:
        check = (check + rng.randint(1, 9)) % 10
    return first12 + str(check)


def is_valid_national_id(value: str) -> bool:
    """Mirror of the dbt ``valid_thai_national_id`` test, for pytest use."""
    if value is None:
        return False
    digits = value.strip().replace("-", "")
    if len(digits) != 13 or not digits.isdigit():
        return False
    return int(digits[12]) == national_id_check_digit(digits[:12])


def make_phone(rng: random.Random, style: str | None = None) -> str:
    """Thai mobile number in one of several real-world surface forms."""
    style = style or rng.choice(["dashed", "intl", "plain", "spaced"])
    sub = f"{rng.randint(0, 9)}{rng.randint(100, 999)}{rng.randint(1000, 9999)}"
    body = sub[0], sub[1:4], sub[4:]
    if style == "dashed":
        return f"08{body[0]}-{body[1]}-{body[2]}"
    if style == "intl":
        return f"+668{body[0]}{body[1]}{body[2]}"
    if style == "spaced":
        return f"08{body[0]} {body[1]} {body[2]}"
    return f"08{body[0]}{body[1]}{body[2]}"


def make_plate(rng: random.Random) -> tuple[str, str]:
    """Return ``(plate, province)`` in the Thai private-vehicle format.

    Example: ``1กข 1234`` registered in ``กรุงเทพมหานคร``.
    """
    lead = rng.randint(1, 9)
    cons = "".join(rng.choice(PLATE_CONSONANTS) for _ in range(2))
    number = rng.randint(1, 9999)
    idx = rng.randrange(len(PROVINCES_TH))
    return f"{lead}{cons} {number}", PROVINCES_TH[idx]


def make_thai_name(rng: random.Random) -> tuple[str, str]:
    return rng.choice(THAI_GIVEN_NAMES), rng.choice(THAI_FAMILY_NAMES)
