#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from lxml import etree

NS = {"hl7": "urn:hl7-org:v2xml"}


# ---------------------------
# XPath helpers
# ---------------------------
def xa(node, xp: str):
    return list(node.xpath(xp, namespaces=NS)) if node is not None else []


def x1(node, xp: str):
    r = node.xpath(xp, namespaces=NS) if node is not None else []
    return r[0] if r else None


def txt(el) -> str:
    return (el.text or "").strip() if el is not None else ""


# ---------------------------
# Structural path signature
# ---------------------------
def localname(el: etree._Element) -> str:
    return etree.QName(el).localname


def indexed_path(el: etree._Element) -> str:
    """
    Absolute path using localname + 1-based index among siblings with same tag.
    Example: /REF_I12[1]/MSH[1]/MSH.7[1]/TS.1[1]
    """
    parts: List[str] = []
    cur: Optional[etree._Element] = el
    while cur is not None:
        parent = cur.getparent()
        if parent is None:
            # root
            idx = 1
        else:
            same = [c for c in parent if c.tag == cur.tag]
            idx = same.index(cur) + 1
        parts.append(f"{localname(cur)}[{idx}]")
        cur = parent
    return "/" + "/".join(reversed(parts))


def element_path_set(tree: etree._ElementTree) -> set[str]:
    root = tree.getroot()
    paths: set[str] = set()
    for el in root.iter():
        # include every element node
        paths.add(indexed_path(el))
    return paths


# ---------------------------
# Observation heading signature
# ---------------------------
def observation_heading_signature(root: etree._Element) -> List[Tuple[str, str, str, List[Tuple[str, str, str]]]]:
    """
    For each REF_I12.OBSERVATION group:
      - OBR-4 (CE.1/CE.2/CE.3)
      - ordered list of OBX-3 (CE.1/CE.2/CE.3) for all OBX in that group
    """
    sig: List[Tuple[str, str, str, List[Tuple[str, str, str]]]] = []

    groups = xa(root, "/hl7:REF_I12/hl7:REF_I12.OBSERVATION")
    if not groups:
        groups = xa(root, "//hl7:REF_I12.OBSERVATION")

    for g in groups:
        obr4_1 = txt(x1(g, "./hl7:OBR/hl7:OBR.4/hl7:CE.1"))
        obr4_2 = txt(x1(g, "./hl7:OBR/hl7:OBR.4/hl7:CE.2"))
        obr4_3 = txt(x1(g, "./hl7:OBR/hl7:OBR.4/hl7:CE.3"))

        obx_ids: List[Tuple[str, str, str]] = []
        for obx in xa(g, ".//hl7:OBX"):
            c1 = txt(x1(obx, "./hl7:OBX.3/hl7:CE.1"))
            c2 = txt(x1(obx, "./hl7:OBX.3/hl7:CE.2"))
            c3 = txt(x1(obx, "./hl7:OBX.3/hl7:CE.3"))
            obx_ids.append((c1, c2, c3))

        sig.append((obr4_1, obr4_2, obr4_3, obx_ids))
    return sig


# ---------------------------
# Count signature (repeat groups)
# ---------------------------
@dataclass(frozen=True)
class Counts:
    procedures: int
    observations: int
    obr: int
    obx: int
    dg1: int
    al1: int
    pr1: int
    pid: int
    pv1: int
    msh: int
    prd: int

def count_signature(root: etree._Element) -> Counts:
    # Use robust fallbacks where needed
    procedures = len(xa(root, "/hl7:REF_I12/hl7:REF_I12.PROCEDURE")) or len(xa(root, "//hl7:REF_I12.PROCEDURE"))
    observations = len(xa(root, "/hl7:REF_I12/hl7:REF_I12.OBSERVATION")) or len(xa(root, "//hl7:REF_I12.OBSERVATION"))
    obr = len(xa(root, "/hl7:REF_I12/hl7:REF_I12.OBSERVATION/hl7:OBR")) or len(xa(root, "//hl7:OBR"))
    obx = len(xa(root, "/hl7:REF_I12/hl7:REF_I12.OBSERVATION//hl7:OBX")) or len(xa(root, "//hl7:OBX"))
    dg1 = len(xa(root, "/hl7:REF_I12/hl7:DG1")) or len(xa(root, "//hl7:DG1"))
    al1 = len(xa(root, "/hl7:REF_I12/hl7:AL1")) or len(xa(root, "//hl7:AL1"))
    pr1 = len(xa(root, "/hl7:REF_I12/hl7:REF_I12.PROCEDURE//hl7:PR1")) or len(xa(root, "//hl7:PR1"))
    pid = len(xa(root, "/hl7:REF_I12/hl7:PID")) or len(xa(root, "//hl7:PID"))
    pv1 = len(xa(root, "/hl7:REF_I12/hl7:REF_I12.PATIENT_VISIT//hl7:PV1")) or len(xa(root, "//hl7:PV1"))
    msh = len(xa(root, "/hl7:REF_I12/hl7:MSH")) or len(xa(root, "//hl7:MSH"))
    prd = len(xa(root, "/hl7:REF_I12/hl7:REF_I12.PROVIDER_CONTACT//hl7:PRD")) or len(xa(root, "//hl7:PRD"))
    return Counts(
        procedures=procedures,
        observations=observations,
        obr=obr,
        obx=obx,
        dg1=dg1,
        al1=al1,
        pr1=pr1,
        pid=pid,
        pv1=pv1,
        msh=msh,
        prd=prd,
    )


# ---------------------------
# Canonical hash for duplicate file detection
# ---------------------------
def canonical_sha256(tree: etree._ElementTree) -> str:
    root = tree.getroot()
    # Canonical XML (C14N) ignores pretty-print differences; ideal for detecting duplicates
    c14n_bytes = etree.tostring(root, method="c14n", exclusive=True, with_comments=False)
    return hashlib.sha256(c14n_bytes).hexdigest()


# ---------------------------
# Extract “should be unique” values
# ---------------------------
def extract_uniques(root: etree._Element) -> Dict[str, str]:
    msh10 = txt(x1(root, "/hl7:REF_I12/hl7:MSH/hl7:MSH.10")) or txt(x1(root, "//hl7:MSH/hl7:MSH.10"))

    # PID.3 is repeated; prefer the first non-IHINumber if present, else first PID.3
    pid3_nodes = xa(root, "/hl7:REF_I12/hl7:PID/hl7:PID.3") or xa(root, "//hl7:PID/hl7:PID.3")
    pid3 = ""
    for p3 in pid3_nodes:
        t = txt(x1(p3, "./hl7:CX.5"))
        v = txt(x1(p3, "./hl7:CX.1"))
        if t != "IHINumber" and v:
            pid3 = v
            break
    if not pid3 and pid3_nodes:
        pid3 = txt(x1(pid3_nodes[0], "./hl7:CX.1"))

    fam = txt(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.5/hl7:XPN.1/hl7:FN.1")) or txt(x1(root, "//hl7:PID.5//hl7:FN.1"))
    giv = txt(x1(root, "/hl7:REF_I12/hl7:PID/hl7:PID.5/hl7:XPN.2")) or txt(x1(root, "//hl7:PID.5/hl7:XPN.2"))

    pv119 = txt(
        x1(root, "/hl7:REF_I12/hl7:REF_I12.PATIENT_VISIT//hl7:PV1/hl7:PV1.19/hl7:CX.1")
        or x1(root, "//hl7:PV1/hl7:PV1.19/hl7:CX.1")
    )

    return {
        "msh10": msh10,
        "pid3": pid3,
        "pname": f"{giv} {fam}".strip(),
        "pv119": pv119,
    }


# ---------------------------
# Signature bundle
# ---------------------------
@dataclass
class TemplateSig:
    top_order: List[str]
    paths: set[str]
    obs: List[Tuple[str, str, str, List[Tuple[str, str, str]]]]
    counts: Counts
    require_prd: bool

def build_template_sig(tree: etree._ElementTree) -> TemplateSig:
    root = tree.getroot()
    top_order = [localname(ch) for ch in list(root)]
    paths = element_path_set(tree)
    obs = observation_heading_signature(root)
    counts = count_signature(root)
    require_prd = counts.prd > 0
    return TemplateSig(top_order=top_order, paths=paths, obs=obs, counts=counts, require_prd=require_prd)


# ---------------------------
# Main validation
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="Validate DS XML outputs against DS_SampleC1 template")
    ap.add_argument("--template", required=True, help="Path to DS_SampleC1.xml")
    ap.add_argument("--indir", required=True, help="Folder containing ds_*.xml outputs")
    ap.add_argument("--expected-count", type=int, default=None, help="Optional: require exactly N xml files")
    ap.add_argument("--no-path-check", action="store_true", help="Disable strict element-path skeleton check (not recommended)")
    ap.add_argument("--no-heading-check", action="store_true", help="Disable OBR-4/OBX-3 heading check")
    ap.add_argument("--no-count-check", action="store_true", help="Disable repeat-count check")
    ap.add_argument("--require-visit-unique", action="store_true", help="Require PV1-19 to be present and unique")
    ap.add_argument("--require-file-unique", action="store_true", default=True,
                    help="Require no duplicate files (canonical XML hash). Default: on")
    ap.add_argument("--max-path-diff", type=int, default=8, help="How many missing/extra paths to print on failure")
    args = ap.parse_args()

    parser = etree.XMLParser(remove_blank_text=True)

    tmpl_tree = etree.parse(args.template, parser)
    tmpl_sig = build_template_sig(tmpl_tree)

    files = [f for f in os.listdir(args.indir) if f.lower().endswith(".xml")]
    if not files:
        raise SystemExit("No .xml files found in --indir")

    if args.expected_count is not None and len(files) != args.expected_count:
        raise SystemExit(f"Expected {args.expected_count} XML files, found {len(files)} in {args.indir}")

    seen_msh10: set[str] = set()
    seen_patient: set[str] = set()
    seen_pv119: set[str] = set()
    seen_filehash: Dict[str, str] = {}  # hash -> filename

    errors = 0

    for fn in sorted(files):
        path = os.path.join(args.indir, fn)
        try:
            tree = etree.parse(path, parser)
        except Exception as e:
            print(f"[FAIL] {fn}: cannot parse XML: {e}")
            errors += 1
            continue

        root = tree.getroot()

        # 0) Duplicate-file check (canonical)
        if args.require_file_unique:
            h = canonical_sha256(tree)
            if h in seen_filehash:
                print(f"[FAIL] {fn}: duplicate XML content (canonical) matches {seen_filehash[h]}")
                errors += 1
                continue
            seen_filehash[h] = fn

        # 1) Top-level order (direct children under REF_I12)
        top = [localname(ch) for ch in list(root)]
        if top != tmpl_sig.top_order:
            print(f"[FAIL] {fn}: top-level segment/group order differs from template")
            print(f"       got:  {top}")
            print(f"       exp:  {tmpl_sig.top_order}")
            errors += 1
            continue

        # 2) Strict skeleton (all element paths + indexes)
        if not args.no_path_check:
            paths = element_path_set(tree)
            if paths != tmpl_sig.paths:
                missing = sorted(tmpl_sig.paths - paths)
                extra = sorted(paths - tmpl_sig.paths)
                print(f"[FAIL] {fn}: element-path skeleton differs from template")
                if missing:
                    print(f"       Missing paths (showing up to {args.max_path_diff}):")
                    for p in missing[: args.max_path_diff]:
                        print(f"         - {p}")
                if extra:
                    print(f"       Extra paths (showing up to {args.max_path_diff}):")
                    for p in extra[: args.max_path_diff]:
                        print(f"         + {p}")
                errors += 1
                continue

        # 3) Repeat-count checks (match template)
        if not args.no_count_check:
            c = count_signature(root)
            if c != tmpl_sig.counts:
                print(f"[FAIL] {fn}: repeat counts differ from template")
                print(f"       got: {c}")
                print(f"       exp: {tmpl_sig.counts}")
                errors += 1
                continue

        # 4) OBR-4 + OBX-3 headings/identifiers (match template, in order)
        if not args.no_heading_check:
            obs = observation_heading_signature(root)
            if len(obs) != len(tmpl_sig.obs):
                print(f"[FAIL] {fn}: number of OBSERVATION groups differs from template")
                errors += 1
                continue

            ok = True
            for i, (a, b) in enumerate(zip(obs, tmpl_sig.obs), start=1):
                if a[0:3] != b[0:3]:
                    print(f"[FAIL] {fn}: OBR-4 identifiers differ in OBSERVATION group #{i}")
                    print(f"       got: {a[0:3]}")
                    print(f"       exp: {b[0:3]}")
                    ok = False
                    break
                if a[3] != b[3]:
                    print(f"[FAIL] {fn}: OBX-3 identifiers/order differ in OBSERVATION group #{i}")
                    ok = False
                    break
            if not ok:
                errors += 1
                continue

        # 5) Required segments present (template-driven)
        if tmpl_sig.counts.msh == 0 or tmpl_sig.counts.pid == 0 or tmpl_sig.counts.pv1 == 0 or tmpl_sig.counts.obr == 0:
            # This shouldn't happen if DS_SampleC1 is sane
            print("[WARN] Template missing key segments; validation assumptions may not hold")

        # If template has PRD, require it
        if tmpl_sig.require_prd:
            if (len(xa(root, "/hl7:REF_I12/hl7:REF_I12.PROVIDER_CONTACT//hl7:PRD")) or len(xa(root, "//hl7:PRD"))) == 0:
                print(f"[FAIL] {fn}: missing PRD, but template contains PRD")
                errors += 1
                continue

        # 6) Uniqueness checks (values)
        u = extract_uniques(root)

        if not u["msh10"] or u["msh10"] in seen_msh10:
            print(f"[FAIL] {fn}: duplicate or missing MSH-10")
            errors += 1
            continue
        seen_msh10.add(u["msh10"])

        patient_key = f"{u['pname']}|{u['pid3']}".upper()
        if not u["pid3"] or not u["pname"] or patient_key in seen_patient:
            print(f"[FAIL] {fn}: duplicate or missing patient name/ID (PID-5 + PID-3)")
            errors += 1
            continue
        seen_patient.add(patient_key)

        if args.require_visit_unique:
            if not u["pv119"] or u["pv119"] in seen_pv119:
                print(f"[FAIL] {fn}: duplicate or missing PV1-19 visit number")
                errors += 1
                continue
            seen_pv119.add(u["pv119"])

        print(f"[OK]   {fn}")

    if errors:
        raise SystemExit(f"\nValidation FAILED with {errors} error(s).")

    print("\nValidation PASSED: structure matches template, headings match, and outputs are unique.")


if __name__ == "__main__":
    main()
