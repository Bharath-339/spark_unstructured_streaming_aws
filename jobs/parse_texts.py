import json
import os

from udf_utils import *

INPUT_DIR = r"../input/input_text"

fields = [
    "file_name",
    "position",
    "classcode",
    "salary_start",
    "salary_end",
    "start_date",
    "end_date",
    "req",
    "notes",
    "duties",
    "selection",
    "experience_length",
    "job_type",
    "education_length",
    "school_type",
    "application_location",
]


def parse_file(path):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    result = {}
    result["file_name"] = extract_file_name(content)
    result["position"] = extract_position(content)
    result["classcode"] = extract_classcode(content)
    salary = extract_salary(content)
    result["salary_start"] = salary[0]
    result["salary_end"] = salary[1]
    sd = extract_start_date(content)
    ed = extract_end_date(content)
    result["start_date"] = sd.isoformat() if sd else None
    result["end_date"] = ed.isoformat() if ed else None
    result["req"] = extract_requirements(content)
    result["notes"] = extract_notes(content)
    result["duties"] = extract_duties(content)
    result["selection"] = extract_selection(content)
    result["experience_length"] = extract_experience_length(content)
    result["job_type"] = extract_job_type(content)
    result["education_length"] = extract_education_length(content)
    result["school_type"] = extract_school_type(content)
    result["application_location"] = extract_application_location(content)
    return result


def main():
    files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith(".txt")]
    all_parsed = []
    for fn in files:
        path = os.path.join(INPUT_DIR, fn)
        parsed = parse_file(path)
        print(json.dumps(parsed, indent=2))
        all_parsed.append(parsed)
    # optionally write to a combined jsonl
    out_path = "../output/output_parsed.jsonl"
    with open(out_path, "w", encoding="utf-8") as out:
        for p in all_parsed:
            out.write(json.dumps(p) + "\n")
    print(f"Wrote combined parsed results to {out_path}")


if __name__ == "__main__":
    main()
