# Discharge Summary Generator (HL7 v2.4 XML / v2xml)

Template-based generator that produces **synthetic HL7 v2.4 Discharge Summary XML** files by mutating values inside a **golden template** (`DS_SampleC1.xml`) while preserving the template’s **exact structure**.

✅ **Structure preserved** (segment/group order, repeating group counts)  
✅ **OBR-4 and OBX-3 identifiers/headings preserved** (kept exactly as in the template)  
✅ **Values regenerated** (patient, provider, visit, narrative) so outputs are **not duplicates**  
✅ **Irish-plausible data** (counties/towns/Eircode pattern/+353 phones)

---

## What this generator does

Given a template HL7 v2.4 XML file (DS_SampleC1.xml), it generates `N` new discharge summaries:

- Keeps the **same XML skeleton** as the template
- Updates **existing nodes only** (no schema redesign, no new node creation)
- Regenerates:
  - Message IDs (unique)
  - Patient demographics + identifiers
  - Provider/GP contact details
  - Visit/encounter details (admit/discharge timestamps, visit number)
  - Diagnoses / procedures / allergies
  - Narrative text sections (OBX-5) while keeping section headings unchanged

---

## Repository layout

templates/
DS_SampleC1.xml # NOT committed by default (add locally)

tools/
ds_from_template_generate.py # main generator script

requirements.txt

yaml
Copy code

> `DS_SampleC1.xml` is typically not committed to the repo.  
> Place it locally at `templates/DS_SampleC1.xml`.

---

## Requirements

- Python 3.10+ recommended
- Install dependencies:

```bash
pip install -r requirements.txt
If you hit lxml errors, ensure lxml is listed in requirements.txt and reinstall.
What stays the same vs what changes
Preserved (identical to DS_SampleC1)

Segment/group structure and ordering

Repeating group counts and positions (e.g., PROCEDURE×2, OBSERVATION×10)

OBR-4 and OBX-3 identifiers/headings and order (template “section headings”)

Mutated (regenerated values)

MSH

MSH-7 timestamp

MSH-10 Message Control ID (unique per file)

MSH-4 sending facility varies

PRD (provider contact)

Provider name, ID, address, phone (Irish formats)

PID (patient)

Name, DOB, sex

Patient identifiers (unique)

Irish address (county/town/Eircode pattern)

Irish phone (+353 formats)

PV1 (encounter)

PV1-19 Visit number (unique)

Admit/discharge timestamps

Facility/location fields (template-specific)

Clinician fields populated in the template’s style

DG1 / PR1 / AL1

Diagnoses, procedures, allergies regenerated

AL1.5 supports both template styles (AL1.5 plain text OR AL1.5/CE.2)

OBR/OBX narrative

Headings stay fixed

Narrative text (OBX-5) regenerated in clinical-style English

Safety / Privacy

All data is synthetic and for testing/demos only

Not suitable for clinical decision-making

Do not commit real patient data to this repository
