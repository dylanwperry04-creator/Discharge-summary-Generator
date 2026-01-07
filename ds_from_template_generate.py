#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import re
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from faker import Faker
from lxml import etree

# HL7 v2 XML namespace (detected from template at runtime when possible)
NS: Dict[str, str] = {"hl7": "urn:hl7-org:v2xml"}

# -------------------------------------------------------------------------
# Reference data (Ireland-focused)
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
# Namespace detection + XPath helpers (read-only; no node creation)
# -------------------------------------------------------------------------

def detect_namespace(tree: etree._ElementTree) -> Dict[str, str]:
    root = tree.getroot()
    if root is None:
        return {}
    tag = root.tag
    if isinstance(tag, str) and tag.startswith("{") and "}" in tag:
        uri = tag.split("}")[0][1:]
        return {"hl7": uri}
    return {}

def _strip_hl7_prefix(xp: str) -> str:
    return xp.replace("hl7:", "")

def x1(node, xp: str):
    if node is None:
        return None
    if NS:
        try:
            r = node.xpath(xp, namespaces=NS)
            if r:
                return r[0]
        except etree.XPathEvalError:
            pass
    try:
        r = node.xpath(_strip_hl7_prefix(xp))
        return r[0] if r else None
    except etree.XPathEvalError:
        return None

def xa(node, xp: str):
    if node is None:
        return []
    if NS:
        try:
            r = node.xpath(xp, namespaces=NS)
            if r:
                return list(r)
        except etree.XPathEvalError:
            pass
    try:
        return list(node.xpath(_strip_hl7_prefix(xp)))
    except etree.XPathEvalError:
        return []

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
# Optional IPS bundle ingestion (FHIR JSON)
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
# Irish address/phone generation
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
# Scenario model (drives diagnosis-aware text, investigations, PR1 procedures)
# -------------------------------------------------------------------------

@dataclass(frozen=True)
class Scenario:
    code: str
    display: str
    system: str
    presentations: List[str]
    tests_core: List[str]
    tests_optional: List[str]

SCENARIOS: Dict[str, Scenario] = {
    "J18.9": Scenario(
        code="J18.9",
        display="Pneumonia, unspecified organism",
        system="I10",
        presentations=[
            "cough, fever and shortness of breath",
            "productive cough with pleuritic chest pain and fever",
            "dyspnoea with raised inflammatory markers",
            "worsening cough and fever over several days",
            "chest tightness with low oxygen saturation on exertion",
        ],
        tests_core=[
            "Bloods: FBC, CRP, U&E, LFTs",
            "Chest X-ray (CXR)",
            "Observations incl. oxygen saturation",
        ],
        tests_optional=[
            "Blood cultures (if febrile/septic)",
            "Sputum culture (if productive cough)",
            "Viral PCR swab (seasonal)",
            "ABG/VBG (if hypoxic)",
            "Lactate (if sepsis suspected)",
        ],
    ),
    "N39.0": Scenario(
        code="N39.0",
        display="Urinary tract infection, site not specified",
        system="I10",
        presentations=[
            "dysuria with urinary frequency and suprapubic discomfort",
            "lower urinary tract symptoms with fever",
            "urinary symptoms with raised inflammatory markers",
            "flank discomfort with urinary symptoms and fever",
            "new urinary frequency with dysuria and malaise",
        ],
        tests_core=[
            "Urinalysis / dipstick",
            "Urine culture & sensitivity (MC&S)",
            "Bloods: FBC, CRP, U&E (if unwell/complicated)",
        ],
        tests_optional=[
            "Blood cultures (if febrile/septic)",
            "Renal ultrasound (if flank pain / obstruction suspected)",
            "Pregnancy test (if relevant)",
        ],
    ),
    "I10": Scenario(
        code="I10",
        display="Essential (primary) hypertension",
        system="I10",
        presentations=[
            "persistently elevated blood pressure readings",
            "hypertension identified during admission assessment",
            "raised blood pressure requiring monitoring and review",
            "elevated blood pressure noted on repeated observations",
            "newly identified hypertension on routine checks",
        ],
        tests_core=[
            "Repeat BP measurements (including correct cuff size/position)",
            "Bloods: U&E/creatinine, electrolytes",
            "Urinalysis (protein/haematuria)",
            "ECG",
        ],
        tests_optional=[
            "HbA1c / fasting glucose",
            "Lipid profile",
            "Urine ACR (albumin:creatinine ratio)",
            "Chest X-ray (if indicated)",
        ],
    ),
    "E11.9": Scenario(
        code="E11.9",
        display="Type 2 diabetes mellitus without complications",
        system="I10",
        presentations=[
            "hyperglycaemia on admission assessment",
            "raised HbA1c suggesting suboptimal glycaemic control",
            "elevated capillary blood glucose readings",
            "hyperglycaemia requiring monitoring and medication review",
        ],
        tests_core=[
            "Capillary blood glucose monitoring",
            "HbA1c",
            "Bloods: U&E/creatinine",
            "Lipid profile",
        ],
        tests_optional=[
            "Urine ACR (albumin:creatinine ratio)",
            "Ketones (if unwell / very high glucose)",
            "ECG (baseline cardiovascular assessment)",
        ],
    ),
    "S72.001A": Scenario(
        code="S72.001A",
        display="Fracture of unspecified part of neck of right femur",
        system="I10",
        presentations=[
            "fall with hip pain and reduced mobility",
            "hip pain following trauma with inability to weight bear",
            "suspected hip fracture after a fall",
            "mechanical fall with immediate hip pain and shortened, externally rotated leg",
        ],
        tests_core=[
            "X-ray: hip/pelvis",
            "Bloods: FBC, U&E/creatinine",
            "Coagulation profile (if indicated)",
            "Group & save / crossmatch (peri-operative planning)",
            "ECG (pre-op assessment)",
        ],
        tests_optional=[
            "CT/MRI hip (if occult fracture suspected)",
            "Chest X-ray (pre-op / if indicated)",
        ],
    ),
}

DX_FALLBACK_POOL: List[Tuple[str, str, str]] = [
    ("I10", "Essential (primary) hypertension", "I10"),
    ("E11.9", "Type 2 diabetes mellitus without complications", "I10"),
    ("J18.9", "Pneumonia, unspecified organism", "I10"),
    ("N39.0", "Urinary tract infection, site not specified", "I10"),
    ("S72.001A", "Fracture of unspecified part of neck of right femur", "I10"),
]

def scenario_from(code: str, display: str) -> Optional[Scenario]:
    c = (code or "").strip().upper()
    if c in SCENARIOS:
        return SCENARIOS[c]
    t = (display or "").lower()
    if "pneumon" in t:
        return SCENARIOS.get("J18.9")
    if "urinar" in t or "uti" in t or "cystitis" in t or "pyelo" in t:
        return SCENARIOS.get("N39.0")
    if "hypertens" in t or "high blood pressure" in t:
        return SCENARIOS.get("I10")
    if "diabet" in t or "hyperglyc" in t:
        return SCENARIOS.get("E11.9")
    if "hip" in t or "femur" in t or "neck of femur" in t or "fracture" in t:
        return SCENARIOS.get("S72.001A")
    return None

def pick_scenario(ips: Optional[Dict[str, Any]], forced_code: Optional[str], used: Dict[str, Any]) -> Scenario:
    if forced_code:
        s = scenario_from(forced_code, forced_code)
        if s:
            return s

    if ips:
        conds = ips.get("conditions") or []
        if conds:
            code, disp, _sys = first_coding(conds[0].get("code") or {})
            s = scenario_from(code, disp)
            if s:
                return s

    used_codes = used.setdefault("scenario_codes", set())
    available = [t for t in DX_FALLBACK_POOL if t[0].upper() not in used_codes]
    code, disp, sysc = random.choice(available if available else DX_FALLBACK_POOL)
    used_codes.add(code.upper())

    s = scenario_from(code, disp)
    if s:
        return s

    return Scenario(
        code=code,
        display=disp,
        system=sysc or "I10",
        presentations=["an acute presentation requiring assessment"],
        tests_core=["Bloods: FBC, U&E", "Clinical assessment and observations"],
        tests_optional=["ECG (if indicated)", "Imaging (if indicated)"],
    )

def render_investigations(s: Scenario) -> Tuple[List[str], str]:
    core = list(dict.fromkeys(s.tests_core))
    opt = list(dict.fromkeys(s.tests_optional))

    k = 0
    if opt:
        k = random.randint(0, min(2, len(opt)))
    chosen_opt = random.sample(opt, k=k) if k else []
    bullets = core + chosen_opt

    intro = random.choice(["Evaluation / investigations:", "Investigations performed:", "Assessment and investigations:"])
    closing = random.choice([
        "Results reviewed and documented; plan discussed as appropriate.",
        "Findings were reviewed and documented; follow-up arranged if needed.",
        "No urgent inpatient abnormalities requiring further work-up were documented.",
        "Investigations supported the working diagnosis; management plan documented.",
    ])

    lines = [intro]
    for b in bullets:
        lines.append(f"- {b}.")
    lines.append(f"- {closing}")
    return bullets, "\n".join(lines)

# -------------------------------------------------------------------------
# PR1 procedure content (scenario-driven; avoids inappropriate CT/MRI brain)
# -------------------------------------------------------------------------

@dataclass(frozen=True)
class ProcedureItem:
    code: str
    label: str
    system: str
    description: str

def procedures_for_scenario(s: Scenario) -> List[ProcedureItem]:
    if s.code == "S72.001A":
        return [
            ProcedureItem(
                code="XR-HIP-PELVIS",
                label="X-ray (Chest/Pelvis/Hip)",
                system="SCT",
                description="X-ray chest, pelvis and right hip performed; findings consistent with hip fracture; chest imaging without acute abnormality.",
            ),
            ProcedureItem(
                code="ECG-12LEAD",
                label="ECG",
                system="SCT",
                description="12-lead ECG performed as part of pre-operative assessment; no acute abnormalities documented.",
            ),
        ]
    if s.code == "I10":
        return [
            ProcedureItem(
                code="CXR-ECG",
                label="Chest X-ray & ECG",
                system="SCT",
                description="Chest X-ray and ECG performed as part of assessment; no acute cardiopulmonary abnormality documented.",
            ),
            ProcedureItem(
                code="BP-MONITOR",
                label="Blood pressure monitoring",
                system="SCT",
                description="Repeated blood pressure measurements performed; elevated readings recorded and management plan documented.",
            ),
        ]
    if s.code == "J18.9":
        return [
            ProcedureItem(
                code="CXR",
                label="Chest X-ray (CXR)",
                system="SCT",
                description="Chest X-ray performed; findings documented and consistent with lower respiratory tract infection.",
            ),
            ProcedureItem(
                code="BLOODS",
                label="Blood tests",
                system="SCT",
                description="Blood tests performed (FBC, CRP and renal profile) to support diagnosis and monitor response to treatment.",
            ),
        ]
    if s.code == "N39.0":
        return [
            ProcedureItem(
                code="URINE",
                label="Urinalysis & urine culture",
                system="SCT",
                description="Urinalysis performed and urine sent for culture & sensitivity (MC&S) as part of UTI work-up.",
            ),
            ProcedureItem(
                code="BLOODS",
                label="Blood tests",
                system="SCT",
                description="Blood tests performed (FBC, CRP and renal profile) where clinically indicated.",
            ),
        ]
    if s.code == "E11.9":
        return [
            ProcedureItem(
                code="HBA1C-LIPIDS",
                label="Blood tests (HbA1c/Lipids)",
                system="SCT",
                description="HbA1c and lipid profile checked / arranged as part of diabetes review; renal profile monitored.",
            ),
            ProcedureItem(
                code="ECG",
                label="ECG",
                system="SCT",
                description="Baseline ECG performed / reviewed as part of cardiovascular risk assessment.",
            ),
        ]
    return [
        ProcedureItem(code="BLOODS", label="Blood tests", system="SCT", description="Blood tests performed and reviewed."),
        ProcedureItem(code="ECG", label="ECG", system="SCT", description="ECG performed if clinically indicated."),
    ]

# -------------------------------------------------------------------------
# Evaluation / investigations headings for OBR group (structure preserved)
# -------------------------------------------------------------------------

def evaluation_headings_for_scenario(s: Scenario) -> List[Tuple[str, str]]:
    if s.code == "J18.9":
        return [
            ("Chest X-ray (CXR)", "CXR: patchy airspace opacification consistent with infection; no pleural effusion."),
            ("Inflammatory markers (WCC/CRP)", "Bloods: raised inflammatory markers; trend improving on treatment."),
            ("Oxygen saturation", "Obs: oxygen saturation monitored; stable on room air / low-flow oxygen as required."),
        ]
    if s.code == "N39.0":
        return [
            ("Urinalysis", "Urinalysis: leukocytes/nitrites positive; findings consistent with UTI."),
            ("Urine culture & sensitivity", "Urine MC&S: sent; results pending / to be reviewed by GP if outstanding."),
            ("Renal function (U&E/Creatinine)", "Bloods: renal function checked; no acute kidney injury documented."),
        ]
    if s.code == "I10":
        return [
            ("Repeat blood pressure measurements", "BP: repeated readings taken; elevated values noted; advice/plan documented."),
            ("ECG", "ECG: no acute ischaemic changes; baseline rhythm documented."),
            ("Renal function & electrolytes", "Bloods: U&E/electrolytes checked; no critical abnormalities documented."),
        ]
    if s.code == "E11.9":
        return [
            ("Capillary blood glucose", "CBG: monitored during admission; values improved with management plan."),
            ("HbA1c", "HbA1c: checked / arranged; suggests glycaemic control requires review."),
            ("Renal function (U&E/Creatinine)", "Bloods: renal function monitored; no acute deterioration documented."),
        ]
    if s.code == "S72.001A":
        return [
            ("X-ray hip/pelvis", "Imaging: X-ray confirms hip fracture; ortho plan documented."),
            ("Pre-op bloods (FBC/U&E)", "Bloods: FBC and U&E performed for operative planning; stable results."),
            ("ECG (pre-op)", "ECG: baseline assessment completed; no acute abnormalities documented."),
        ]
    return [
        ("Clinical assessment", "Clinical assessment completed; observations monitored."),
        ("Bloods", "Blood tests performed and reviewed."),
        ("Imaging (if indicated)", "Imaging arranged as appropriate."),
    ]

# -------------------------------------------------------------------------
# Allergy generation (consistent category/type; includes NKA option)
# -------------------------------------------------------------------------

ALLERGY_CATS = {
    "DRUG": {"cat_code": "DA", "type_text": "DRUG", "allergens": ["Penicillin", "Aspirin", "Contrast media"]},
    "FOOD": {"cat_code": "FA", "type_text": "FOOD", "allergens": ["Peanuts", "Shellfish"]},
    "ENVIRONMENTAL": {"cat_code": "EA", "type_text": "ENVIRONMENTAL", "allergens": ["Latex", "Pollen"]},
    "NKA": {"cat_code": "NA", "type_text": "N/A", "allergens": ["No known allergy"]},
}

REACTION_POOL = ["Rash", "Urticaria", "Angioedema", "Wheeze", "Anaphylaxis", "Nausea", "Vomiting", "Rhinitis"]
SEVERITY_POOL = ["MILD", "MODERATE", "SEVERE"]

def classify_allergy(allergen_text: str) -> str:
    t = (allergen_text or "").strip().lower()
    if not t:
        return "DRUG"
    if "no known" in t or t in {"none", "nka"}:
        return "NKA"
    if any(w in t for w in ["penicillin", "aspirin", "contrast"]):
        return "DRUG"
    if any(w in t for w in ["peanut", "shellfish", "nut"]):
        return "FOOD"
    if any(w in t for w in ["latex", "pollen", "dust", "mite"]):
        return "ENVIRONMENTAL"
    return random.choice(["DRUG", "FOOD", "ENVIRONMENTAL"])

def pick_allergy(ips_allergen: Optional[str]) -> Tuple[str, str, str, str, str]:
    if ips_allergen:
        cat = classify_allergy(ips_allergen)
        allergen = ips_allergen.strip()
    else:
        if random.random() < 0.12:
            cat = "NKA"
        else:
            cat = random.choice(["DRUG", "FOOD", "ENVIRONMENTAL"])
        allergen = random.choice(ALLERGY_CATS[cat]["allergens"])

    cat_code = ALLERGY_CATS[cat]["cat_code"]
    type_text = ALLERGY_CATS[cat]["type_text"]

    if cat == "NKA":
        return cat_code, type_text, allergen, "N/A", "N/A"

    severity = random.choice(SEVERITY_POOL)
    reaction = random.choice(REACTION_POOL)
    return cat_code, type_text, allergen, severity, reaction

# -------------------------------------------------------------------------
# Narrative helpers (reduce repetition across generated files)
# -------------------------------------------------------------------------

def _pick_unique(used: Dict[str, Any], key: str, options: List[str]) -> str:
    if not options:
        return ""
    text_used = used.setdefault("text_used", {})
    bucket = text_used.setdefault(key, set())
    remaining = [o for o in options if o not in bucket]
    choice = random.choice(remaining if remaining else options)
    bucket.add(choice)
    return choice

def medications_for_scenario(s: Scenario) -> List[str]:
    if s.code == "S72.001A":
        return [
            "Morphine (as required for pain)",
            "Heparin for VTE prophylaxis (as per protocol)"
        ]
    if s.code == "I10":
        return [
            "Losartan 50mg OD",
            "Amlodipine 5mg OD (if required)",
            "Atorvastatin 20mg ON (if indicated)",
        ]
    if s.code == "N39.0":
        return [
            "Nitrofurantoin 100mg BD (as per local guidance)",
            "Paracetamol 1g QDS PRN",
        ]
    if s.code == "J18.9":
        return [
            "Doxycycline 100mg OD (as per local guidance)",
            "Paracetamol 1g QDS PRN",
        ]
    if s.code == "E11.9":
        return [
            "Metformin 500mg BD with food",
            "Atorvastatin 20mg ON (if indicated)",
        ]
    return ["Paracetamol 1g QDS PRN"]

def section_text(fake: Faker, section_label: str, ips: Optional[Dict[str, Any]], scenario: Scenario, used: Dict[str, Any]) -> str:
    lab = (section_label or "").lower().strip()

    if ips:
        if "medication" in lab and "withheld" not in lab:
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

        if "diagnos" in lab or "problem" in lab:
            conds = ips.get("conditions") or []
            if conds:
                lines = []
                for c in conds[:12]:
                    _, disp, _ = first_coding(c.get("code") or {})
                    if disp:
                        lines.append(f"- {disp}")
                if lines:
                    return "Problem list:\n" + "\n".join(lines)

    if "summary" in lab:
        presentation = _pick_unique(used, f"{scenario.code}:presentation", scenario.presentations or ["an acute presentation"])
        return "\n".join([
            _pick_unique(used, "summary:hdr", ["Discharge summary:", "Discharge summary (brief):", "Discharge summary (overview):"]),
            f"Admitted with {presentation}; treated and improved during admission.",
            _pick_unique(used, "summary:closing", [
                "Discharged home with follow-up plan and safety-net advice.",
                "Medication list reconciled and discharge instructions provided.",
                "Discharged in stable condition with GP follow-up arranged.",
                "Follow-up plan provided with return precautions discussed.",
            ]),
        ])

    if "hospital course" in lab:
        return "\n".join([
            _pick_unique(used, "course:line1", [
                "Hospital course: assessed by the admitting team and managed per local protocol.",
                "Hospital course: monitored and treated during admission with clinical improvement.",
                "Hospital course: work-up completed and condition stabilised prior to discharge.",
                "Hospital course: clinical assessment and investigations completed; treatment plan implemented.",
            ]),
            f"Primary issue addressed: {scenario.display}.",
            _pick_unique(used, "course:line3", [
                "Observations remained stable; afebrile at discharge where applicable.",
                "Tolerating oral intake; mobilising as tolerated prior to discharge.",
                "Pain controlled with appropriate analgesia as required.",
                "Symptoms improved prior to discharge with stable vital signs.",
            ]),
            _pick_unique(used, "course:line4", [
                "No complications reported during stay.",
                "No adverse events documented.",
                "Discharged in stable condition.",
                "Clinical status stable; discharge criteria met.",
            ]),
        ])

    if "evaluation" in lab or "investig" in lab:
        _bullets, inv_text = render_investigations(scenario)
        return inv_text

    if "risk" in lab:
        return "\n".join([
            "Risk factors:",
            _pick_unique(used, "risk:smoke", ["- Smoking: non-smoker.", "- Smoking: current smoker; cessation advice given.", "- Smoking: ex-smoker."]),
            _pick_unique(used, "risk:alcohol", ["- Alcohol: minimal.", "- Alcohol: moderate intake.", "- Alcohol: none reported."]),
            _pick_unique(used, "risk:lifestyle", ["- Activity: encouraged to mobilise as tolerated.", "- Diet: advice provided as appropriate.", "- Weight: lifestyle advice provided as appropriate."]),
        ])

    if "adverse" in lab:
        return _pick_unique(used, "adverse", [
            "Adverse events: none reported.",
            "Adverse events: no documented complications during admission.",
            "Adverse events: mild nausea post-medication; resolved without intervention.",
            "Adverse events: none documented.",
        ])

    if "medication" in lab and "withheld" not in lab:
        meds = medications_for_scenario(scenario)
        random.shuffle(meds)
        keep = meds[: random.randint(2, min(5, len(meds)))]
        return "Medications on discharge:\n" + "\n".join(f"- {m}" for m in keep)

    if "withheld" in lab:
        return _pick_unique(used, f"withheld:{scenario.code}", [
            "Medications withheld: none.",
            "Medications withheld: NSAIDs avoided due to renal function — GP to review.",
            "Medications withheld: anticoagulant held temporarily — GP to review.",
        ])

    if "hospital action" in lab or "hospital actions" in lab:
        return "\n".join([
            "Hospital actions:",
            _pick_unique(used, "hospact:1", ["- Medication reconciliation completed.", "- Discharge letter prepared and sent to GP.", "- Follow-up clinic arranged if required."]),
            _pick_unique(used, "hospact:2", ["- Results reviewed and documented.", "- Patient provided with written advice and plan.", "- Safety-net advice discussed and documented."]),
        ])

    if "gp action" in lab or "gp actions" in lab or "follow" in lab:
        return "\n".join([
            "GP actions / follow-up:",
            f"GP review within {_pick_unique(used, 'gp:window', ['3–5', '5–7', '7–10', '10–14'])} days.",
            _pick_unique(used, "gp:review", ["Review symptoms and response to treatment.", "Review medication tolerance and adherence.", "Review outstanding results if applicable."]),
            _pick_unique(used, "gp:safetynet", ["Return to ED if worsening symptoms, chest pain, persistent fever, or new concerns.", "Safety-net advice provided (seek urgent care if deterioration)."]),
        ])

    if "clinic info" in lab or "information given" in lab:
        return "\n".join([
            "Clinic / discharge information:",
            _pick_unique(used, "info:1", ["Discharge letter provided to patient.", "Discharge summary provided and explained."]),
            _pick_unique(used, "info:2", ["Medication plan and follow-up arrangements explained.", "Medication plan reviewed; follow-up arranged."]),
            _pick_unique(used, "info:3", ["Advice provided on symptoms to monitor and when to seek urgent care.", "Patient understands return precautions."]),
        ])

    return "\n".join([
        "Clinical narrative:",
        _pick_unique(used, "narr:1", ["Patient stable at discharge.", "Symptoms improved prior to discharge.", "No acute concerns at discharge."]),
        _pick_unique(used, "narr:2", ["Follow-up arranged with GP.", "Safety-net advice provided.", "Medication plan reviewed."]),
    ])

# -------------------------------------------------------------------------
# Template-preserving mutation (updates values only; does not create nodes)
# -------------------------------------------------------------------------

def mutate_tree(
    tree: etree._ElementTree,
    fake: Faker,
    ips: Optional[Dict[str, Any]],
    used: Dict[str, Any],
    forced_scenario: Optional[str],
    train_writer=None,
) -> etree._ElementTree:
    root = tree.getroot()

    msg_id = str(uuid.uuid4())
    visit_id = str(random.randint(10**8, 10**9 - 1))
    base_filler = str(random.randint(10**9, 10**10 - 1))

    while msg_id in used.setdefault("msh10", set()):
        msg_id = str(uuid.uuid4())
    used["msh10"].add(msg_id)

    while visit_id in used.setdefault("pv119", set()):
        visit_id = str(random.randint(10**8, 10**9 - 1))
    used["pv119"].add(visit_id)

    while base_filler in used.setdefault("obr3", set()):
        base_filler = str(random.randint(10**9, 10**10 - 1))
    used["obr3"].add(base_filler)

    sending_hospital = random.choice(IRISH_HOSPITALS)
    care_hospital = random.choice(IRISH_HOSPITALS)

    scenario = pick_scenario(ips, forced_scenario, used)
    canon_tests, canon_inv_text = render_investigations(scenario)

    set_text(x1(root, "/hl7:REF_I12/hl7:MSH/hl7:MSH.7/hl7:TS.1"), hl7_ts())
    set_text(x1(root, "/hl7:REF_I12/hl7:MSH/hl7:MSH.10"), msg_id)
    set_text(x1(root, "/hl7:REF_I12/hl7:MSH/hl7:MSH.4/hl7:HD.1"), sending_hospital)

    gp_given = fake.first_name()
    gp_family = fake.last_name()
    gp_id = str(random.randint(100000, 999999))
    receiving_str = f"{gp_family.upper()}, {gp_given}"

    msh6 = x1(root, "/hl7:REF_I12/hl7:MSH/hl7:MSH.6")
    if msh6 is not None:
        set_text(x1(msh6, "hl7:HD.1"), receiving_str)
        set_text(x1(msh6, "hl7:HD.2"), f"{gp_id}.1234")

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
    while pname_key in used.setdefault("patientkey", set()):
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

    line1, town, county, eir = irish_address(fake)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.11/hl7:XAD.1/hl7:SAD.1"), line1)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.11/hl7:XAD.2"), town)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.11/hl7:XAD.3"), county)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.11/hl7:XAD.5"), eir)
    set_text(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.13/hl7:XTN.1"), irish_phone())

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
        set_text(x1(pv1, "./hl7:PV1.3/hl7:PL.9"), care_hospital)

        dld1 = x1(pv1, "./hl7:PV1.37/hl7:DLD.1")
        if dld1 is not None:
            set_text(dld1, str(random.randint(100000, 999999)))
        else:
            set_text(x1(pv1, "./hl7:PV1.37"), str(random.randint(100000, 999999)))

        doc_title = random.choice(["DR", "PROF", "MR", "MS"])
        doc_given = safe_upper(fake.first_name())
        doc_family = safe_upper(fake.last_name())

        f7 = x1(pv1, "./hl7:PV1.7")
        if f7 is not None:
            set_text(x1(f7, "./hl7:XCN.1"), safe_upper(care_hospital) + " 1")
            set_text(x1(f7, "./hl7:XCN.2/hl7:FN.1"), doc_family)
            set_text(x1(f7, "./hl7:XCN.3"), doc_given)
            set_text(x1(f7, "./hl7:XCN.6"), doc_title)

        f8 = x1(pv1, "./hl7:PV1.8")
        if f8 is not None:
            set_text(x1(f8, "./hl7:XCN.1"), " ")
            set_text(x1(f8, "./hl7:XCN.2/hl7:FN.1"), doc_family)
            set_text(x1(f8, "./hl7:XCN.3"), doc_given)
            set_text(x1(f8, "./hl7:XCN.6"), doc_title)

        f9 = x1(pv1, "./hl7:PV1.9")
        if f9 is not None:
            short_code = doc_family[:4] if len(doc_family) >= 4 else doc_family
            combined_name = f"{doc_family} {doc_given}"
            set_text(x1(f9, "./hl7:XCN.1"), short_code)
            set_text(x1(f9, "./hl7:XCN.2/hl7:FN.1"), combined_name)
            set_text(x1(f9, "./hl7:XCN.3"), "")
            set_text(x1(f9, "./hl7:XCN.6"), "")

    dg1s = xa(root, "/hl7:REF_I12/hl7:DG1")
    if not dg1s:
        dg1s = xa(root, "//hl7:DG1")

    ips_conds = (ips.get("conditions") if ips else None) or []
    for i, dg in enumerate(dg1s, start=1):
        set_text(x1(dg, "./hl7:DG1.16/hl7:XCN.2/hl7:FN.1"), "")
        set_text(x1(dg, "./hl7:DG1.16/hl7:XCN.3"), "")
        set_text(x1(dg, "./hl7:DG1.16/hl7:XCN.6"), "")

        if i == 1:
            code, disp, sysc = scenario.code, scenario.display, scenario.system
        elif ips_conds and i <= len(ips_conds):
            code, disp, sysc = first_coding(ips_conds[i - 1].get("code") or {})
            if not sysc:
                sysc = "SCT"
            if not code:
                code = f"DX{random.randint(1000, 9999)}"
            if not disp:
                disp = "Condition"
        else:
            code, disp, sysc = random.choice(DX_FALLBACK_POOL)

        set_text(x1(dg, "./hl7:DG1.1"), str(i))
        set_text(x1(dg, "./hl7:DG1.3/hl7:CE.1"), code)
        set_text(x1(dg, "./hl7:DG1.3/hl7:CE.2"), disp)
        set_text(x1(dg, "./hl7:DG1.3/hl7:CE.3"), sysc)
        set_text(x1(dg, "./hl7:DG1.4"), disp)

    al1s = xa(root, "/hl7:REF_I12/hl7:AL1")
    if not al1s:
        al1s = xa(root, "//hl7:AL1")

    ips_alls = (ips.get("allergies") if ips else None) or []
    multi_al1 = len(al1s) > 1

    for i, al1 in enumerate(al1s, start=1):
        ips_text = None
        if ips_alls and i <= len(ips_alls):
            _, disp, _ = first_coding(ips_alls[i - 1].get("code") or {})
            ips_text = disp or None

        cat_code, type_text, allergen, severity, reaction = pick_allergy(ips_text)

        if multi_al1 and cat_code == "NA":
            cat_code, type_text, allergen, severity, reaction = pick_allergy(None)
            if cat_code == "NA":
                cat_code, type_text, allergen, severity, reaction = ("DA", "DRUG", "Penicillin", random.choice(SEVERITY_POOL), random.choice(REACTION_POOL))

        set_text(x1(al1, "./hl7:AL1.1"), str(i))
        set_text(x1(al1, "./hl7:AL1.1/hl7:CE.1"), str(i))

        set_text(x1(al1, "./hl7:AL1.2/hl7:CE.1"), cat_code)
        set_text(x1(al1, "./hl7:AL1.2/hl7:CE.2"), type_text)

        set_text(x1(al1, "./hl7:AL1.3/hl7:CE.2"), allergen)
        set_text(x1(al1, "./hl7:AL1.4/hl7:CE.2"), severity)

        al15_ce2 = x1(al1, "./hl7:AL1.5/hl7:CE.2")
        if al15_ce2 is not None:
            set_text(al15_ce2, reaction)
        else:
            set_text(x1(al1, "./hl7:AL1.5"), reaction)

        if allergen.strip().lower().startswith("no known"):
            set_text(x1(al1, "./hl7:AL1.4/hl7:CE.2"), "N/A")
            if al15_ce2 is not None:
                set_text(al15_ce2, "N/A")
            else:
                set_text(x1(al1, "./hl7:AL1.5"), "N/A")

    proc_groups = xa(root, "/hl7:REF_I12/hl7:REF_I12.PROCEDURE")
    if not proc_groups:
        proc_groups = xa(root, "//hl7:REF_I12.PROCEDURE")

    proc_items = procedures_for_scenario(scenario)
    for idx, pg in enumerate(proc_groups):
        pr1 = x1(pg, ".//hl7:PR1")
        if pr1 is None:
            continue
        item = proc_items[idx % len(proc_items)]
        set_text(x1(pr1, "./hl7:PR1.3/hl7:CE.1"), item.code)
        set_text(x1(pr1, "./hl7:PR1.3/hl7:CE.2"), item.label)
        set_text(x1(pr1, "./hl7:PR1.3/hl7:CE.3"), item.system)
        set_text(x1(pr1, "./hl7:PR1.4"), item.description)

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

        sec_lower = section_label.lower()
        group_is_eval = (
            ("evaluat" in sec_lower and "procedure" in sec_lower)
            or ("evaluat" in sec_lower and "investig" in sec_lower)
            or ("investig" in sec_lower)
        )

        obxs = xa(g, ".//hl7:OBX")

        if group_is_eval:
            inv_pairs = evaluation_headings_for_scenario(scenario)
            inv_i = 0

            for obx in obxs:
                obx2 = x1(obx, "./hl7:OBX.2")
                dtype = (obx2.text if obx2 is not None else "").strip().upper()
                if dtype not in ("FT", "TX", "ST"):
                    continue

                obx5 = x1(obx, "./hl7:OBX.5")
                if obx5 is None:
                    continue

                heading, result_text = inv_pairs[inv_i % len(inv_pairs)]
                inv_i += 1

                set_text(x1(obx, "./hl7:OBX.3/hl7:CE.2"), heading)
                set_text(obx5, result_text)

        else:
            for obx in obxs:
                obx2 = x1(obx, "./hl7:OBX.2")
                dtype = (obx2.text if obx2 is not None else "").strip().upper()
                if dtype not in ("FT", "TX", "ST"):
                    continue

                obx5 = x1(obx, "./hl7:OBX.5")
                if obx5 is None:
                    continue

                obx3el = x1(obx, "./hl7:OBX.3/hl7:CE.2")
                obx3_label = (obx3el.text.strip() if (obx3el is not None and obx3el.text) else "")

                label = obx3_label or section_label or "Narrative"
                set_text(obx5, section_text(fake, label, ips, scenario, used))

    if train_writer is not None:
        train_writer.write(json.dumps({
            "message_control_id": msg_id,
            "visit_id": visit_id,
            "scenario_code": scenario.code,
            "scenario_display": scenario.display,
            "canonical_tests": canon_tests,
            "investigations_narrative": canon_inv_text,
        }, ensure_ascii=False) + "\n")

    return tree

# -------------------------------------------------------------------------
# CLI entrypoint
# -------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True, help="DS_SampleC1.xml template path")
    ap.add_argument("--outdir", required=True, help="Output folder")
    ap.add_argument("--count", type=int, default=10, help="How many DS messages to generate")
    ap.add_argument("--seed", type=int, default=None, help="Optional RNG seed")
    ap.add_argument("--ips", default=None, help="Optional IPS bundle JSON to populate from")
    ap.add_argument("--scenario", default=None, help="Force primary scenario code (e.g. J18.9, N39.0, I10, E11.9, S72.001A)")
    ap.add_argument("--train_out", default=None, help="Optional JSONL training output path")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    os.makedirs(args.outdir, exist_ok=True)

    fake = Faker("en_IE")
    if args.seed is not None:
        fake.seed_instance(args.seed)

    used: Dict[str, Any] = {"msh10": set(), "pv119": set(), "obr3": set(), "patientkey": set(), "scenario_codes": set()}

    ips = load_ips(args.ips) if args.ips else None
    base_tree = etree.parse(args.template)

    global NS
    detected = detect_namespace(base_tree)
    NS = detected if detected else {}

    train_writer = None
    if args.train_out:
        os.makedirs(os.path.dirname(args.train_out) or ".", exist_ok=True)
        train_writer = open(args.train_out, "w", encoding="utf-8")

    try:
        for i in range(1, args.count + 1):
            tree = deepcopy(base_tree)
            mutate_tree(tree, fake, ips, used, args.scenario, train_writer=train_writer)
            out_path = os.path.join(args.outdir, f"ds_{i:03d}.xml")
            xml_bytes = etree.tostring(
                tree.getroot(),
                xml_declaration=True,
                encoding="utf-8",
                pretty_print=True,
            )
            with open(out_path, "wb") as f:
                f.write(xml_bytes)
    finally:
        if train_writer is not None:
            train_writer.close()

    print(f"Generated {args.count} Discharge Summary files into: {args.outdir}")
    if args.train_out:
        print(f"Wrote training JSONL to: {args.train_out}")

if __name__ == "__main__":
    main()
