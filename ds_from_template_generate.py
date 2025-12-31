#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import re
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from faker import Faker
from lxml import etree

NS = {"hl7": "urn:hl7-org:v2xml"}

# -------------------------------------------------------------------------
# Irish-safe reference data
# -------------------------------------------------------------------------

IRISH_HOSPITALS: List[str] = [
    "St James's Hospital",
    "Beaumont Hospital",
    "Mater Misericordiae University Hospital",
    "St Vincent's University Hospital",
    "Tallaght University Hospital",
    "Cork University Hospital",
    "University Hospital Galway",
    "University Hospital Limerick",
    "University Hospital Waterford",
    "Sligo University Hospital",
    "Our Lady of Lourdes Hospital",
    "Connolly Hospital Blanchardstown",
    "St Luke's General Hospital Kilkenny",
    "Wexford General Hospital",
    "Letterkenny University Hospital",
    "Mayo University Hospital",
    "Portiuncula University Hospital",
    "St Vincents Hospital",
    "Cavan Hospital",
    "St Lukes Hospital",
    "Mercy University Hospital Cork",
]

IRISH_COUNTIES: List[str] = [
    "CARLOW", "CAVAN", "CLARE", "CORK", "DONEGAL", "DUBLIN", "GALWAY", "KERRY",
    "KILDARE", "KILKENNY", "LAOIS", "LEITRIM", "LIMERICK", "LONGFORD", "LOUTH",
    "MAYO", "MEATH", "MONAGHAN", "OFFALY", "ROSCOMMON", "SLIGO", "TIPPERARY",
    "WATERFORD", "WESTMEATH", "WEXFORD", "WICKLOW",
]

TOWNS_BY_COUNTY: Dict[str, List[str]] = {
    "CARLOW": ["Carlow", "Tullow", "Bagenalstown"],
    "CAVAN": ["Cavan", "Bailieborough", "Virginia"],
    "CLARE": ["Ennis", "Shannon", "Kilrush"],
    "CORK": ["Cork", "Mallow", "Midleton", "Bandon", "Clonakilty", "Youghal"],
    "DONEGAL": ["Letterkenny", "Buncrana", "Donegal", "Ballybofey"],
    "DUBLIN": ["Dublin", "Swords", "Tallaght", "Clondalkin", "Dún Laoghaire", "Blanchardstown"],
    "GALWAY": ["Galway", "Tuam", "Loughrea", "Ballinasloe", "Oranmore"],
    "KERRY": ["Tralee", "Killarney", "Dingle", "Listowel"],
    "KILDARE": ["Naas", "Newbridge", "Maynooth", "Kildare"],
    "KILKENNY": ["Kilkenny", "Thomastown", "Castlecomer"],
    "LAOIS": ["Portlaoise", "Portarlington", "Mountmellick"],
    "LEITRIM": ["Carrick-on-Shannon", "Manorhamilton", "Ballinamore"],
    "LIMERICK": ["Limerick", "Newcastle West", "Kilmallock", "Adare"],
    "LONGFORD": ["Longford", "Granard", "Edgeworthstown"],
    "LOUTH": ["Dundalk", "Drogheda", "Ardee"],
    "MAYO": ["Castlebar", "Ballina", "Westport"],
    "MEATH": ["Navan", "Trim", "Kells", "Ashbourne"],
    "MONAGHAN": ["Monaghan", "Carrickmacross", "Castleblayney"],
    "OFFALY": ["Tullamore", "Birr", "Edenderry"],
    "ROSCOMMON": ["Roscommon", "Boyle", "Castlerea"],
    "SLIGO": ["Sligo", "Tubbercurry", "Ballymote"],
    "TIPPERARY": ["Nenagh", "Thurles", "Clonmel", "Tipperary"],
    "WATERFORD": ["Waterford", "Dungarvan", "Tramore"],
    "WESTMEATH": ["Mullingar", "Athlone", "Moate"],
    "WEXFORD": ["Wexford", "Gorey", "Enniscorthy", "New Ross"],
    "WICKLOW": ["Wicklow", "Bray", "Greystones", "Arklow"],
}

EIRCODE_ROUTING_KEYS = [
    "D01", "D02", "D03", "D04", "D05", "D06", "D07", "D08", "D09", "D10", "D11", "D12", "D13", "D14", "D15", "D16",
    "D17", "D18", "D20", "D22", "D24",
    "T12", "T23", "T34", "T45", "T56",
    "H91", "V94", "V92", "F92", "F91", "A94", "A96", "K67", "R95", "X91",
    "V42", "V31", "P85", "N37", "Y35",
]
EIRCODE_RE = re.compile(r"^[A-Z0-9]{3}\s?[A-Z0-9]{4}$")


# -------------------------------------------------------------------------
# XML helpers (no node creation; only set existing template nodes)
# -------------------------------------------------------------------------

def x1(node, xp: str):
    r = node.xpath(xp, namespaces=NS) if node is not None else []
    return r[0] if r else None


def xa(node, xp: str):
    return list(node.xpath(xp, namespaces=NS)) if node is not None else []


def set_text(el, txt: Optional[str]):
    if el is not None:
        el.text = txt if txt is not None else ""


def hl7_ts(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.strftime("%Y%m%d%H%M%S")


def hl7_date(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def safe_upper(s: str) -> str:
    return (s or "").upper()


def gender_code(g: str) -> str:
    g = (g or "").lower()
    if g.startswith("m"):
        return "M"
    if g.startswith("f"):
        return "F"
    return "U"


# -------------------------------------------------------------------------
# IPS helpers
# -------------------------------------------------------------------------

def load_ips(ips_path: str) -> Dict[str, Any]:
    with open(ips_path, "r", encoding="utf-8") as f:
        bundle = json.load(f)
    entries = bundle.get("entry") or []
    resources = [e.get("resource") for e in entries if e.get("resource")]
    return {
        "patient": next((r for r in resources if r.get("resourceType") == "Patient"), None),
        "allergies": [r for r in resources if r.get("resourceType") == "AllergyIntolerance"],
        "conditions": [r for r in resources if r.get("resourceType") == "Condition"],
        "meds": [r for r in resources if r.get("resourceType") == "MedicationStatement"],
        "procedures": [r for r in resources if r.get("resourceType") == "Procedure"],
        "immunizations": [r for r in resources if r.get("resourceType") == "Immunization"],
        "observations": [r for r in resources if r.get("resourceType") == "Observation"],
    }


def first_coding(codeable: Any) -> Tuple[str, str, str]:
    """
    Returns (code, display, system_code)
    system_code: SCT/LN/I10/RXNORM/L
    """
    if not codeable:
        return "", "", ""
    coding = (codeable.get("coding") or [])
    c0 = coding[0] if coding else None
    if not c0:
        return "", (codeable.get("text") or ""), ""
    code = c0.get("code") or ""
    display = c0.get("display") or (codeable.get("text") or "")
    system = (c0.get("system") or "").lower()
    if "snomed" in system:
        sysc = "SCT"
    elif "loinc" in system:
        sysc = "LN"
    elif "icd" in system:
        sysc = "I10"
    elif "rxnorm" in system:
        sysc = "RXNORM"
    else:
        sysc = "L" if system else ""
    return code, display, sysc


# -------------------------------------------------------------------------
# Irish address/phone helpers
# -------------------------------------------------------------------------

def irish_eircode() -> str:
    rk = random.choice(EIRCODE_ROUTING_KEYS)
    tail = "".join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789") for _ in range(4))
    e = f"{rk} {tail}"
    return e if EIRCODE_RE.match(e) else "D02 X285"


def irish_phone() -> str:
    r = random.random()
    if r < 0.70:
        prefix = random.choice(["083", "085", "086", "087", "089"])
        return f"+353 {prefix[1:]} {random.randint(1000000, 9999999)}"
    if r < 0.85:
        return f"+353 1 {random.randint(1000000, 9999999)}"
    area = random.choice(["21", "51", "61", "91", "65", "74"])
    return f"+353 {area} {random.randint(100000, 999999)}"


def choose_county_and_town() -> Tuple[str, str]:
    county = random.choice(IRISH_COUNTIES)
    town = random.choice(TOWNS_BY_COUNTY.get(county, [county.title()]))
    return county, town


def irish_address(fake: Faker) -> Tuple[str, str, str, str]:
    county, town = choose_county_and_town()
    house = str(random.randint(1, 250))
    street = fake.street_name()
    line1 = f"{house} {street}".upper()
    eir = irish_eircode().upper()
    return line1, town.upper(), county.upper(), eir


# -------------------------------------------------------------------------
# Narrative generation (IPS-first where possible)
# -------------------------------------------------------------------------

def section_text(fake: Faker, section_label: str, ips: Optional[Dict[str, Any]]) -> str:
    lab = (section_label or "").lower().strip()

    # IPS-first mapping
    if ips:
        if "medication" in lab:
            meds = ips.get("meds") or []
            if meds:
                lines = []
                for m in meds[:12]:
                    _, disp, _ = first_coding(m.get("medicationCodeableConcept") or {})
                    if disp:
                        lines.append(f"- {disp}")
                if lines:
                    return "Medications on discharge:\n" + "\n".join(lines)

        if "allerg" in lab:
            alls = ips.get("allergies") or []
            if alls:
                lines = []
                for a in alls[:12]:
                    _, disp, _ = first_coding(a.get("code") or {})
                    if disp:
                        lines.append(f"- {disp}")
                if lines:
                    return "Allergies:\n" + "\n".join(lines)

        if "diagnos" in lab or "problem" in lab or "summary" in lab:
            conds = ips.get("conditions") or []
            if conds:
                lines = []
                for c in conds[:12]:
                    _, disp, _ = first_coding(c.get("code") or {})
                    if disp:
                        lines.append(f"- {disp}")
                if lines:
                    return "Problem list:\n" + "\n".join(lines)

    def pick(options):
        return random.choice(options)

    if "summary" in lab:
        return "\n".join([
            "Discharge summary:",
            pick([
                "Admitted with an acute presentation; treated and improved during admission.",
                "Admitted for assessment and management; symptoms improved prior to discharge.",
                "Admitted under the medical team; work-up completed and condition stabilised.",
            ]),
            pick([
                "Discharged home with follow-up plan and safety-net advice.",
                "Medication list reconciled and discharge instructions provided.",
                "Discharged in stable condition with GP follow-up arranged.",
            ]),
        ])

    if "risk" in lab:
        return "\n".join([
            "Risk factors:",
            pick(["- Smoking: non-smoker.", "- Smoking: current smoker; cessation advice given.", "- Smoking: ex-smoker."]),
            pick(["- Alcohol: minimal.", "- Alcohol: moderate intake.", "- Alcohol: none reported."]),
            pick(["- Activity: encouraged to mobilise as tolerated.", "- Diet: advice provided as appropriate."]),
        ])

    if "adverse" in lab:
        return pick([
            "Adverse events: none reported.",
            "Adverse events: no documented complications during admission.",
            "Adverse events: mild nausea post-medication; resolved without intervention.",
        ])

    if "hospital course" in lab:
        return "\n".join([
            pick([
                "Hospital course: symptoms improved with treatment.",
                "Hospital course: stable throughout admission.",
                "Hospital course: gradual improvement over the admission.",
            ]),
            pick([
                "Observations remained stable; afebrile at discharge.",
                "Pain controlled with simple analgesia.",
                "Tolerating oral intake; mobilising independently prior to discharge.",
            ]),
            pick([
                "No complications reported during stay.",
                "No adverse events documented.",
                "Discharged in stable condition.",
            ]),
        ])

    if "evaluation" in lab or "investigation" in lab:
        return "\n".join([
            "Evaluation / investigations:",
            pick(["- Bloods: FBC, U&E, LFTs.", "- Bloods: FBC, CRP, U&E.", "- Bloods: U&E, glucose, CRP."]),
            pick(["- ECG: normal sinus rhythm.", "- Chest X-ray: no acute abnormality.", "- Imaging: findings reviewed and documented."]),
            pick(["- Results discussed with patient as appropriate.", "- No urgent abnormalities requiring inpatient follow-up."]),
        ])

    if "medication" in lab and "withheld" not in lab:
        meds = [
            "Paracetamol 1g QDS PRN",
            "Ibuprofen 400mg TDS with food (if tolerated)",
            "Amoxicillin 500mg TDS (course as prescribed)",
            "Omeprazole 20mg OD",
            "Atorvastatin 20mg ON",
            "Ramipril 2.5mg OD",
        ]
        random.shuffle(meds)
        keep = meds[: random.randint(3, 5)]
        return "Medications on discharge:\n" + "\n".join(f"- {m}" for m in keep)

    if "withheld" in lab:
        return pick([
            "Medications withheld: none.",
            "Medications withheld: NSAIDs avoided due to renal function — GP to review.",
            "Medications withheld: anticoagulant held temporarily — GP to review.",
        ])

    if "hospital action" in lab or "hospital actions" in lab:
        return "\n".join([
            "Hospital actions:",
            pick(["- Medication reconciliation completed.", "- Discharge letter prepared and sent to GP.", "- Follow-up clinic arranged if required."]),
            pick(["- Results reviewed and documented.", "- Patient provided with written advice and plan."]),
        ])

    if "gp action" in lab or "gp actions" in lab or "follow" in lab:
        return "\n".join([
            "GP actions / follow-up:",
            f"GP review within {pick(['3–5', '5–7', '7–10'])} days.",
            pick(["Review symptoms and response to treatment.", "Review medication tolerance and adherence.", "Review outstanding results if applicable."]),
            pick(["Return to ED if worsening symptoms, chest pain, persistent fever, or new concerns.", "Safety-net advice provided (seek urgent care if deterioration)."]),
        ])

    if "clinic info" in lab or "information given" in lab:
        return "\n".join([
            "Clinic / discharge information:",
            "Discharge letter provided to patient.",
            "Medication plan and follow-up arrangements explained.",
            pick(["Advice provided on symptoms to monitor and when to seek urgent care.", "Patient understands return precautions."]),
        ])

    return "\n".join([
        "Clinical narrative:",
        pick(["Patient stable at discharge.", "Symptoms improved prior to discharge.", "No acute concerns at discharge."]),
        pick(["Follow-up arranged with GP.", "Safety-net advice provided.", "Medication plan reviewed."]),
    ])


# -------------------------------------------------------------------------
# Mutator (combined “best of both” + fixes AL1.5 plain-text template behaviour)
# -------------------------------------------------------------------------

def mutate_tree(tree: etree._ElementTree, fake: Faker, ips: Optional[Dict[str, Any]], used: Dict[str, set]) -> etree._ElementTree:
    root = tree.getroot()

    # ---------- Unique IDs ----------
    msg_id = str(uuid.uuid4())
    visit_id = str(random.randint(10**8, 10**9 - 1))
    base_filler = str(random.randint(10**9, 10**10 - 1))

    while msg_id in used["msh10"]:
        msg_id = str(uuid.uuid4())
    used["msh10"].add(msg_id)

    while visit_id in used["pv119"]:
        visit_id = str(random.randint(10**8, 10**9 - 1))
    used["pv119"].add(visit_id)

    while base_filler in used["obr3"]:
        base_filler = str(random.randint(10**9, 10**10 - 1))
    used["obr3"].add(base_filler)

    # ---------------------------------------------------------------------
    # Select TWO facilities to match sample "Hub-and-Spoke" / Inconsistency
    # MSH.4 uses 'sending_hospital'
    # PID.3, PV1.3 use 'care_hospital' (can be different, per DS_SampleC1)
    # ---------------------------------------------------------------------
    sending_hospital = random.choice(IRISH_HOSPITALS)
    care_hospital = random.choice(IRISH_HOSPITALS) # May differ from sending_hospital

    # ---------- MSH ----------
    set_text(x1(root, "/hl7:REF_I12/hl7:MSH/hl7:MSH.7/hl7:TS.1"), hl7_ts())
    set_text(x1(root, "/hl7:REF_I12/hl7:MSH/hl7:MSH.10"), msg_id)

    # MSH.4 HD.1 = sending facility (e.g., St Vincents)
    set_text(x1(root, "/hl7:REF_I12/hl7:MSH/hl7:MSH.4/hl7:HD.1"), sending_hospital)

    # MSH.6 = receiving provider (GP) and sync PRD
    gp_given = fake.first_name()
    gp_family = fake.last_name()
    gp_id = str(random.randint(100000, 999999))
    # matches the “PETERS, Peter” style better than all-caps
    receiving_str = f"{gp_family.upper()}, {gp_given}"

    msh6 = x1(root, "/hl7:REF_I12/hl7:MSH/hl7:MSH.6")
    if msh6 is not None:
        set_text(x1(msh6, "hl7:HD.1"), receiving_str)
        set_text(x1(msh6, "hl7:HD.2"), f"{gp_id}.1234")

    # ---------- PRD (sync to MSH.6 GP) ----------
    prds = xa(root, "/hl7:REF_I12/hl7:REF_I12.PROVIDER_CONTACT//hl7:PRD")
    if not prds:
        prds = xa(root, "/hl7:REF_I12//hl7:PRD")

    for prd in prds:
        set_text(x1(prd, "./hl7:PRD.2/hl7:XPN.1/hl7:FN.1"), gp_family.upper())
        set_text(x1(prd, "./hl7:PRD.2/hl7:XPN.2"), gp_given)
        set_text(x1(prd, "./hl7:PRD.7/hl7:PI.1"), gp_id)
        set_text(x1(prd, "./hl7:PRD.7/hl7:PI.3"), receiving_str)

        pl1, ptown, pcounty, peir = irish_address(fake)
        set_text(x1(prd, "./hl7:PRD.3/hl7:XAD.1/hl7:SAD.1"), pl1)
        set_text(x1(prd, "./hl7:PRD.3/hl7:XAD.2"), ptown)
        set_text(x1(prd, "./hl7:PRD.3/hl7:XAD.3"), pcounty)
        set_text(x1(prd, "./hl7:PRD.3/hl7:XAD.5"), peir)
        set_text(x1(prd, "./hl7:PRD.5/hl7:XTN.1"), irish_phone())

    # ---------- PID ----------
    if ips and ips.get("patient"):
        p = ips["patient"]
        name0 = (p.get("name") or [{}])[0]
        given = (name0.get("given") or [""])[0]
        family = name0.get("family") or ""
        dob = (p.get("birthDate") or "1970-01-01").replace("-", "")
        sex = gender_code(p.get("gender") or "U")
    else:
        sex = random.choice(["M", "F"])
        given = fake.first_name_male() if sex == "M" else fake.first_name_female()
        family = fake.last_name()
        dob = hl7_date(fake.date_of_birth(minimum_age=1, maximum_age=95))

    mrn = f"MRN{random.randint(1000000, 9999999)}"
    pname_key = f"{given} {family}|{mrn}".upper()
    while pname_key in used["patientkey"]:
        sex = random.choice(["M", "F"])
        given = fake.first_name_male() if sex == "M" else fake.first_name_female()
        family = fake.last_name()
        mrn = f"MRN{random.randint(1000000, 9999999)}"
        pname_key = f"{given} {family}|{mrn}".upper()
    used["patientkey"].add(pname_key)

    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.5/hl7:XPN.1/hl7:FN.1"), safe_upper(family))
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.5/hl7:XPN.2"), safe_upper(given))
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.7/hl7:TS.1"), dob)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.8"), sex)

    # PID.3 repeat: update all; keep IHINumber special; MRN gets CX.4/HD.1
    # **NOTE**: Using care_hospital for MRN assigning authority (Matches DS_SampleC1 PID.3)
    pid3s = xa(root, "/hl7:REF_I12/hl7:PID/hl7:PID.3")
    if not pid3s:
        pid3s = xa(root, "//hl7:PID.3")

    for pid3 in pid3s:
        cx5 = x1(pid3, "./hl7:CX.5")
        if cx5 is not None and (cx5.text or "").strip() == "IHINumber":
            set_text(x1(pid3, "./hl7:CX.1"), str(random.randint(10**17, 10**18 - 1)))
        else:
            set_text(x1(pid3, "./hl7:CX.1"), mrn)
            set_text(x1(pid3, "./hl7:CX.4/hl7:HD.1"), care_hospital)

    # Irish address + phone
    line1, town, county, eir = irish_address(fake)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.11/hl7:XAD.1/hl7:SAD.1"), line1)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.11/hl7:XAD.2"), town)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.11/hl7:XAD.3"), county)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.11/hl7:XAD.5"), eir)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.13/hl7:XTN.1"), irish_phone())

    # ---------- PV1 ----------
    now = datetime.now(timezone.utc)
    admit = fake.date_time_between(start_date=now - timedelta(days=60), end_date=now - timedelta(days=2), tzinfo=timezone.utc)
    discharge = admit + timedelta(hours=random.randint(12, 240))
    if discharge > now - timedelta(days=1):
        discharge = now - timedelta(days=1)

    pv1 = x1(root, "/hl7:REF_I12/hl7:REF_I12.PATIENT_VISIT/hl7:PV1")
    if pv1 is None:
        pv1 = x1(root, "/hl7:REF_I12//hl7:PV1")

    if pv1 is not None:
        set_text(x1(pv1, "./hl7:PV1.19/hl7:CX.1"), visit_id)
        set_text(x1(pv1, "./hl7:PV1.44/hl7:TS.1"), hl7_ts(admit))
        set_text(x1(pv1, "./hl7:PV1.45/hl7:TS.1"), hl7_ts(discharge))
        set_text(x1(pv1, "./hl7:PV1.36"), random.choice(["01", "02", "03", "04"]))

        # PV1.3 PL.9 = facility/location string (Using care_hospital to match PID.3)
        set_text(x1(pv1, "./hl7:PV1.3/hl7:PL.9"), care_hospital)

        # PV1.37 (some templates use DLD.1)
        dld1 = x1(pv1, "./hl7:PV1.37/hl7:DLD.1")
        if dld1 is not None:
            set_text(dld1, str(random.randint(100000, 999999)))
        else:
            set_text(x1(pv1, "./hl7:PV1.37"), str(random.randint(100000, 999999)))

        # -----------------------------------------------------------
        # Overwrite PV1.7/8/9 clinician (SPECIAL FORMATTING LOGIC)
        # -----------------------------------------------------------
        doc_title = random.choice(["DR", "PROF", "MR", "MS"])
        doc_given = safe_upper(fake.first_name())
        doc_family = safe_upper(fake.last_name())

        # PV1.7 (Attending) - XCN.1 holds Care Hospital Name + " 1" to match "ST. LUKE'S 1"
        # Names in XCN.2/3, Title in XCN.6
        f7 = x1(pv1, "./hl7:PV1.7")
        if f7 is not None:
            set_text(x1(f7, "./hl7:XCN.1"), safe_upper(care_hospital) + " 1")
            set_text(x1(f7, "./hl7:XCN.2/hl7:FN.1"), doc_family)
            set_text(x1(f7, "./hl7:XCN.3"), doc_given)
            set_text(x1(f7, "./hl7:XCN.6"), doc_title)

        # PV1.8 (Referring) - XCN.1 is empty/space, Names in XCN.2/3, Title in XCN.6
        f8 = x1(pv1, "./hl7:PV1.8")
        if f8 is not None:
            set_text(x1(f8, "./hl7:XCN.1"), " ") 
            set_text(x1(f8, "./hl7:XCN.2/hl7:FN.1"), doc_family)
            set_text(x1(f8, "./hl7:XCN.3"), doc_given)
            set_text(x1(f8, "./hl7:XCN.6"), doc_title)

        # PV1.9 (Consulting) - XCN.1 short code, XCN.2 has Combined Name, XCN.3 empty
        f9 = x1(pv1, "./hl7:PV1.9")
        if f9 is not None:
            short_code = doc_family[:4] if len(doc_family) >= 4 else doc_family
            combined_name = f"{doc_family}  {doc_given}" # Double space style as per sample
            
            set_text(x1(f9, "./hl7:XCN.1"), short_code)
            set_text(x1(f9, "./hl7:XCN.2/hl7:FN.1"), combined_name)
            set_text(x1(f9, "./hl7:XCN.3"), "") # Clear XCN.3
            set_text(x1(f9, "./hl7:XCN.6"), "") # Clear Title

    # ---------- DG1 ----------
    dg1s = xa(root, "/hl7:REF_I12/hl7:DG1")
    if not dg1s:
        dg1s = xa(root, "//hl7:DG1")

    dx_pool = [
        ("I10", "Essential (primary) hypertension", "I10"),
        ("E11.9", "Type 2 diabetes mellitus without complications", "I10"),
        ("J18.9", "Pneumonia, unspecified organism", "I10"),
        ("N39.0", "Urinary tract infection, site not specified", "I10"),
        ("S72.001A", "Fracture of unspecified part of neck of right femur", "I10"),
    ]
    ips_conds = (ips.get("conditions") if ips else None) or []

    for i, dg in enumerate(dg1s, start=1):
        # Clear DG1.16 clinician residue
        set_text(x1(dg, "./hl7:DG1.16/hl7:XCN.2/hl7:FN.1"), "")
        set_text(x1(dg, "./hl7:DG1.16/hl7:XCN.3"), "")
        set_text(x1(dg, "./hl7:DG1.16/hl7:XCN.6"), "")

        if ips_conds and i <= len(ips_conds):
            code, disp, sysc = first_coding(ips_conds[i - 1].get("code") or {})
            if not sysc:
                sysc = "SCT"
            if not code:
                code = f"DX{random.randint(1000, 9999)}"
            if not disp:
                disp = "Condition"
        else:
            code, disp, sysc = random.choice(dx_pool)

        set_text(x1(dg, "./hl7:DG1.1"), str(i))
        set_text(x1(dg, "./hl7:DG1.3/hl7:CE.1"), code)
        set_text(x1(dg, "./hl7:DG1.3/hl7:CE.2"), disp)
        set_text(x1(dg, "./hl7:DG1.3/hl7:CE.3"), sysc)
        set_text(x1(dg, "./hl7:DG1.4"), disp)

    # ---------- PR1 ----------
    pr1s = xa(root, "/hl7:REF_I12/hl7:REF_I12.PROCEDURE/hl7:PR1")
    if not pr1s:
        pr1s = xa(root, "//hl7:PR1")

    proc_pool = [
        ("80146002", "Appendectomy", "SCT"),
        ("233604007", "CT of head", "SCT"),
        ("52734007", "Hip replacement", "SCT"),
        ("73761001", "Chest X-ray", "SCT"),
    ]
    ips_procs = (ips.get("procedures") if ips else None) or []

    for i, pr1 in enumerate(pr1s, start=1):
        if ips_procs and i <= len(ips_procs):
            code, disp, sysc = first_coding(ips_procs[i - 1].get("code") or {})
            if not sysc:
                sysc = "SCT"
            if not code:
                code = f"PR{random.randint(1000, 9999)}"
            if not disp:
                disp = "Procedure"
        else:
            code, disp, sysc = random.choice(proc_pool)

        set_text(x1(pr1, "./hl7:PR1.1"), str(i))
        set_text(x1(pr1, "./hl7:PR1.3/hl7:CE.1"), code)
        set_text(x1(pr1, "./hl7:PR1.3/hl7:CE.2"), disp)
        set_text(x1(pr1, "./hl7:PR1.3/hl7:CE.3"), sysc)
        set_text(x1(pr1, "./hl7:PR1.5/hl7:TS.1"), hl7_ts(admit + timedelta(hours=random.randint(1, 48))))

    # ---------- AL1 (MASTER FIX: handles BOTH AL1.5 text and AL1.5/CE.2) ----------
    al1s = xa(root, "/hl7:REF_I12/hl7:AL1")
    if not al1s:
        al1s = xa(root, "//hl7:AL1")

    allergy_pool = ["Penicillin", "Peanuts", "Latex", "Shellfish", "Aspirin", "Contrast media", "Pollen"]
    reaction_pool = ["Rash", "Urticaria", "Angioedema", "Wheeze", "Anaphylaxis", "Nausea", "Vomiting", "Rhinitis"]
    severity_pool = ["MILD", "MODERATE", "SEVERE"]
    ips_alls = (ips.get("allergies") if ips else None) or []

    # reduce within-message repetition where possible
    reaction_choices = reaction_pool[:]
    random.shuffle(reaction_choices)

    for i, al1 in enumerate(al1s, start=1):
        # allergen (IPS-first)
        if ips_alls and i <= len(ips_alls):
            _, disp, _ = first_coding(ips_alls[i - 1].get("code") or {})
            allergen = disp or random.choice(allergy_pool)
        else:
            allergen = random.choice(allergy_pool)

        severity = random.choice(severity_pool)
        reaction = reaction_choices[(i - 1) % len(reaction_choices)] if reaction_choices else random.choice(reaction_pool)

        # AL1.1 (some templates use plain text; some use CE.1)
        set_text(x1(al1, "./hl7:AL1.1"), str(i))
        set_text(x1(al1, "./hl7:AL1.1/hl7:CE.1"), str(i))

        # AL1.2 (type) if present
        al12_ce1 = x1(al1, "./hl7:AL1.2/hl7:CE.1")
        al12_ce2 = x1(al1, "./hl7:AL1.2/hl7:CE.2")
        if al12_ce1 is not None:
            set_text(al12_ce1, random.choice(["DA", "FA", "EA"]))  # Drug/Food/Environmental (compact)
        if al12_ce2 is not None:
            set_text(al12_ce2, random.choice(["DRUG", "FOOD", "ENVIRONMENTAL"]))

        # AL1.3 allergen (Martha referenced CE.2)
        set_text(x1(al1, "./hl7:AL1.3/hl7:CE.2"), allergen)

        # AL1.4 severity
        set_text(x1(al1, "./hl7:AL1.4/hl7:CE.2"), severity)

        # AL1.5 reaction:
        #   - if template has AL1.5/CE.2, set that
        #   - else set AL1.5 text directly (this is why “Constant runny Nose!” was sticking)
        al15_ce2 = x1(al1, "./hl7:AL1.5/hl7:CE.2")
        if al15_ce2 is not None:
            set_text(al15_ce2, reaction)
        else:
            set_text(x1(al1, "./hl7:AL1.5"), reaction)

    # ---------- OBR/OBX blocks ----------
    obs_groups = xa(root, "/hl7:REF_I12/hl7:REF_I12.OBSERVATION")
    if not obs_groups:
        obs_groups = xa(root, "//hl7:REF_I12.OBSERVATION")

    for idx, g in enumerate(obs_groups, start=1):
        filler_id = f"{base_filler}{idx:02d}"
        set_text(x1(g, "./hl7:OBR/hl7:OBR.3/hl7:EI.1"), filler_id)
        set_text(x1(g, "./hl7:OBR/hl7:OBR.7/hl7:TS.1"), hl7_ts(discharge))
        set_text(x1(g, "./hl7:OBR/hl7:OBR.22/hl7:TS.1"), hl7_ts(discharge))

        section_label = ""
        el = x1(g, "./hl7:OBR/hl7:OBR.4/hl7:CE.2")
        if el is not None and el.text:
            section_label = el.text.strip()

        obxs = xa(g, ".//hl7:OBX")
        for obx in obxs:
            obx2 = x1(obx, "./hl7:OBX.2")
            dtype = (obx2.text if obx2 is not None else "").strip().upper()
            if dtype not in ("FT", "TX", "ST"):
                continue

            obx5 = x1(obx, "./hl7:OBX.5")
            if obx5 is None:
                continue

            obx3el = x1(obx, "./hl7:OBX.3/hl7:CE.2")
            label = (obx3el.text.strip() if (obx3el is not None and obx3el.text) else section_label or "Narrative")
            set_text(obx5, section_text(fake, label, ips))

    return tree


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True, help="DS_SampleC1.xml template path")
    ap.add_argument("--outdir", required=True, help="Output folder")
    ap.add_argument("--count", type=int, default=10, help="How many DS messages to generate")
    ap.add_argument("--seed", type=int, default=None, help="Optional RNG seed")
    ap.add_argument("--ips", default=None, help="Optional IPS bundle JSON to populate from")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    os.makedirs(args.outdir, exist_ok=True)

    fake = Faker("en_IE")
    if args.seed is not None:
        fake.seed_instance(args.seed)

    used = {"msh10": set(), "pv119": set(), "obr3": set(), "patientkey": set()}

    ips = load_ips(args.ips) if args.ips else None
    base_tree = etree.parse(args.template)

    for i in range(1, args.count + 1):
        tree = deepcopy(base_tree)
        mutate_tree(tree, fake, ips, used)
        out_path = os.path.join(args.outdir, f"ds_{i:03d}.xml")
        xml_bytes = etree.tostring(
            tree.getroot(),
            xml_declaration=True,
            encoding="utf-8",
            pretty_print=True,
        )
        with open(out_path, "wb") as f:
            f.write(xml_bytes)

    print(f"Generated {args.count} Discharge Summary files into: {args.outdir}")


if __name__ == "__main__":
    main()