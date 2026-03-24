#!/usr/bin/env python3
"""
College Scorecard API Schema Inspector
Validates API response structure before building extraction logic.
"""

import requests
import json

API_KEY = "oHByS1HOahWshBi23IkWXGpqeYyU77lYmP7XY0Qz"
BASE_URL = "https://api.data.gov/ed/collegescorecard/v1/schools"

def inspect_api():
    """Fetch sample data and inspect structure."""
    
    params = {
        "api_key": API_KEY,
        "per_page": 5
    }
    
    print("=" * 60)
    print("COLLEGE SCORECARD API SCHEMA VALIDATION")
    print("=" * 60)
    
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code != 200:
        print(f"ERROR: API returned status {response.status_code}")
        print(response.text)
        return
    
    data = response.json()
    
    print("\n[1] TOP-LEVEL STRUCTURE")
    print("-" * 40)
    print(f"Keys: {list(data.keys())}")
    
    print("\n[2] METADATA")
    print("-" * 40)
    print(f"Total records: {data['metadata']['total']}")
    print(f"Per page: {data['metadata']['per_page']}")
    
    print("\n[3] RESULTS STRUCTURE")
    print("-" * 40)
    result = data['results'][0]
    print(f"First result keys: {list(result.keys())}")
    
    print("\n[4] 'latest' SECTION KEYS")
    print("-" * 40)
    latest_keys = list(result['latest'].keys())
    print(f"Keys in 'latest': {latest_keys}")
    
    print("\n[5] FIELD VERIFICATION")
    print("-" * 40)
    
    fields_to_verify = [
        ("school.name", ["latest", "school", "name"]),
        ("school.state", ["latest", "school", "state"]),
        ("latest.student.size", ["latest", "student", "size"]),
        ("latest.admissions.admission_rate.overall", ["latest", "admissions", "admission_rate", "overall"]),
        ("latest.completion.rate_suppressed.overall", ["latest", "completion", "rate_suppressed", "overall"]),
    ]
    
    for field_name, path in fields_to_verify:
        value = result
        exists = True
        actual_path = []
        key = ""  # Initialize for Pylance
        
        for key in path:
            if isinstance(value, dict) and key in value:
                value = value[key]
                actual_path.append(key)
            else:
                exists = False
                break
        
        if exists:
            print(f"  ✓ {field_name}")
            print(f"    Path: {'.'.join(actual_path)}")
            print(f"    Sample value: {value}")
        else:
            print(f"  ✗ {field_name} - NOT FOUND")
            print(f"    Searched path: {'.'.join(actual_path)} (stopped at '{key}')")
            
            # Show available keys at failure point
            if isinstance(value, dict):
                print(f"    Available keys at this level: {list(value.keys())[:10]}")
    
    print("\n[6] DETAILED STRUCTURE DUMP")
    print("-" * 40)
    
    print("\n>>> latest.school keys:")
    if 'school' in result['latest']:
        print(f"    {list(result['latest']['school'].keys())}")
    
    print("\n>>> latest.student keys:")
    if 'student' in result['latest']:
        student_keys = list(result['latest']['student'].keys())[:15]
        print(f"    {student_keys}")
    
    print("\n>>> latest.admissions structure:")
    if 'admissions' in result['latest']:
        adm = result['latest']['admissions']
        print(f"    Keys: {list(adm.keys())}")
        if 'admission_rate' in adm:
            print(f"    admission_rate: {adm['admission_rate']}")
    else:
        print("    'admissions' NOT FOUND in latest")
    
    print("\n>>> latest.completion structure:")
    if 'completion' in result['latest']:
        comp = result['latest']['completion']
        comp_keys = list(comp.keys())[:15]
        print(f"    Keys (first 15): {comp_keys}")
        if 'rate_suppressed' in comp:
            print(f"    rate_suppressed: {comp['rate_suppressed']}")
    else:
        print("    'completion' NOT FOUND in latest")
    
    print("\n[7] SAMPLE RECORDS (first 3)")
    print("-" * 40)
    for i, r in enumerate(data['results'][:3]):
        school = r['latest']['school']
        student = r['latest']['student']
        adm = r['latest'].get('admissions', {})
        comp = r['latest'].get('completion', {})
        
        print(f"\n  Record {i+1}:")
        print(f"    Name: {school.get('name')}")
        print(f"    State: {school.get('state')}")
        print(f"    Student Size: {student.get('size')}")
        
        adm_rate = adm.get('admission_rate', {})
        print(f"    Admission Rate: {adm_rate.get('overall') if adm_rate else 'N/A'}")
        
        rate_supp = comp.get('rate_suppressed', {})
        print(f"    Completion Rate: {rate_supp.get('overall') if rate_supp else 'N/A'}")
    
    print("\n" + "=" * 60)
    print("SCHEMA INSPECTION COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    inspect_api()
