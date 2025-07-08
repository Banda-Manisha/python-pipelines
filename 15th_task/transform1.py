#transform.py
import re
 
def parse_resume_text(text):
    lines = text.splitlines()
    name = lines[0].strip() if lines else "Unknown"
 
    email_match = re.search(r'[\w\.-]+@[\w\.-]+', text)
    email = email_match.group() if email_match else "Not found"
 
    experience = []
    summary = []
    skills = []
    education = []
 
    capture_exp = False
    capture_summary = False
    capture_skills = False
    capture_education = False
 
    for line in lines:
        line = line.strip()
 
        if 'Education' in line:
            capture_education = True
            capture_skills = capture_exp = capture_summary = False
            continue
        if 'Technical Skills' in line or 'Skills' in line:
            capture_skills = True
            capture_education = capture_exp = capture_summary = False
            continue
        if 'Professional Experience' in line:
            capture_exp = True
            capture_education = capture_skills = capture_summary = False
            continue
        if 'Projects' in line:
            capture_summary = True
            capture_exp = capture_education = capture_skills = False
            continue
        if 'Certifications' in line:
            capture_summary = capture_exp = capture_education = capture_skills = False
            continue
 
        if capture_exp:
            experience.append(line)
        elif capture_summary:
            summary.append(line)
        elif capture_skills:
            skills.append(line)
        elif capture_education:
            education.append(line)
 
    return {
        'Name': name,
        'Email': email,
        'Summary': '\n'.join(summary).strip(),
        'Experience': '\n'.join(experience).strip(),
        'Skills': '\n'.join(skills).strip(),
        'Education': '\n'.join(education).strip()
    }
 