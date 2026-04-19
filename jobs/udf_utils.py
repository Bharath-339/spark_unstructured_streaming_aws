import re
from datetime import datetime


def extract_file_name(file_content):
    if not file_content:
        return None
    lines = [l for l in file_content.splitlines() if l.strip()]
    return lines[0].strip() if lines else None


def extract_position(file_content):
    if not file_content:
        return None
    lines = [l for l in file_content.splitlines() if l.strip()]
    # second non-empty line is the position/title
    return lines[1].strip() if len(lines) > 1 else None


def extract_classcode(file_content):
    if not file_content:
        return None
    try:
        m = re.search(
            r"Class\s*Code\s*:\s*([A-Za-z0-9_-]+)", file_content, re.IGNORECASE
        )
        return m.group(1).strip() if m else None
    except Exception:
        return None


def extract_salary(file_content):
    """
    Return a tuple (salary_start, salary_end) as floats or (None, None).
    Expected format in the text files: "Salary: 70000.0 - 90000.0"
    """
    if not file_content:
        return (None, None)
    try:
        m = re.search(
            r"Salary\s*:\s*([0-9,\.]+)\s*-\s*([0-9,\.]+)", file_content, re.IGNORECASE
        )
        if not m:
            return (None, None)
        s = m.group(1).replace(",", "")
        e = m.group(2).replace(",", "")
        try:
            s_val = float(s)
        except Exception:
            s_val = None
        try:
            e_val = float(e)
        except Exception:
            e_val = None
        return (s_val, e_val)
    except Exception:
        return (None, None)


def extract_start_date(file_content):
    if not file_content:
        return None
    try:
        m = re.search(
            r"Open\s*Date\s*:\s*(\d{4}-\d{2}-\d{2})", file_content, re.IGNORECASE
        )
        if not m:
            return None
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception:
        return None


def extract_end_date(file_content):
    if not file_content:
        return None
    try:
        m = re.search(
            r"Close\s*Date\s*:\s*(\d{4}-\d{2}-\d{2})", file_content, re.IGNORECASE
        )
        if not m:
            return None
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception:
        return None


def _extract_label_value(file_content, label):
    if not file_content:
        return None
    try:
        # capture remainder of the line after the label
        pattern = rf"{re.escape(label)}\s*:\s*(.*)"
        m = re.search(pattern, file_content, re.IGNORECASE)
        if not m:
            return None
        return m.group(1).strip()
    except Exception:
        return None


def extract_requirements(file_content):
    return _extract_label_value(file_content, "Requirements")


def extract_notes(file_content):
    return _extract_label_value(file_content, "Notes")


def extract_duties(file_content):
    return _extract_label_value(file_content, "Duties")


def extract_selection(file_content):
    return _extract_label_value(file_content, "Selection")


def extract_experience_length(file_content):
    return _extract_label_value(file_content, "Experience Length")


def extract_job_type(file_content):
    return _extract_label_value(file_content, "Job Type")


def extract_education_length(file_content):
    return _extract_label_value(file_content, "Education Length")


def extract_school_type(file_content):
    return _extract_label_value(file_content, "School Type")


def extract_application_location(file_content):
    return _extract_label_value(file_content, "Application Location")
