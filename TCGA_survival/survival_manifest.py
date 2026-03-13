# download_tcga.py (FIXED - One slide per patient)
import requests
import json
import pandas as pd
import os
from pathlib import Path

def download_tcga_complete(project='TCGA-BRCA', n_slides=50, output_dir='tcga_data',
                          one_slide_per_patient=True):
    """
    Download TCGA data for any cancer type
    
    Args:
        project: TCGA project ID
        n_slides: Number of slides (None = all available)
        output_dir: Directory to save outputs
        one_slide_per_patient: If True, keeps only one slide per patient (default: True)
    """
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("="*60)
    print(f"Downloading {project}")
    print(f"Saving to: {output_dir.absolute()}")
    print("="*60)
    
    # ============================================================
    # STEP 1: Get survival data FIRST
    # ============================================================
    print("\n" + "="*60)
    print("STEP 1: Getting survival/clinical data")
    print("="*60)
    
    cases_endpt = 'https://api.gdc.cancer.gov/cases'
    
    filters_cases = {
        "op": "in",
        "content": {
            "field": "cases.project.project_id",
            "value": [project]
        }
    }
    
    params = {
        "filters": json.dumps(filters_cases),
        "fields": "case_id,submitter_id,diagnoses.days_to_death,diagnoses.days_to_last_follow_up,diagnoses.vital_status,diagnoses.age_at_diagnosis,diagnoses.tumor_stage,diagnoses.tumor_grade,diagnoses.primary_diagnosis,demographic.vital_status",
        "format": "JSON",
        "size": "5000"
    }
    
    response = requests.get(cases_endpt, params=params)
    cases = response.json()['data']['hits']
    
    print(f"Found clinical data for {len(cases)} patients")
    
    # Parse survival data
    survival_data = []
    skipped = 0
    
    for case in cases:
        patient_id = case['submitter_id']
        
        if not case.get('diagnoses') or len(case['diagnoses']) == 0:
            skipped += 1
            continue
        
        diag = case['diagnoses'][0]
        
        # Get vital status
        vital_status = diag.get('vital_status')
        if not vital_status and case.get('demographic'):
            vital_status = case['demographic'].get('vital_status')
        
        # Extract days
        days_to_death = diag.get('days_to_death')
        days_to_followup = diag.get('days_to_last_follow_up')
        
        # Convert to int if needed
        if isinstance(days_to_death, str):
            try:
                days_to_death = int(float(days_to_death))
            except:
                days_to_death = None
        
        if isinstance(days_to_followup, str):
            try:
                days_to_followup = int(float(days_to_followup))
            except:
                days_to_followup = None
        
        # Determine survival time and event
        survival_days = None
        event = None
        
        if days_to_death is not None and days_to_death > 0:
            survival_days = days_to_death
            event = 1
        elif days_to_followup is not None and days_to_followup > 0:
            survival_days = days_to_followup
            if vital_status and ('dead' in vital_status.lower() or 'deceased' in vital_status.lower()):
                event = 1
            else:
                event = 0
        else:
            skipped += 1
            continue
        
        survival_data.append({
            'patient_id': patient_id,
            'case_id': case['case_id'],
            'survival_days': survival_days,
            'survival_months': survival_days / 30.44,
            'survival_years': survival_days / 365.25,
            'event': event,
            'vital_status': vital_status,
            'age_at_diagnosis_days': diag.get('age_at_diagnosis'),
            'age_at_diagnosis_years': diag.get('age_at_diagnosis') / 365.25 if diag.get('age_at_diagnosis') else None,
            'tumor_stage': diag.get('tumor_stage'),
            'tumor_grade': diag.get('tumor_grade'),
            'primary_diagnosis': diag.get('primary_diagnosis')
        })
    
    print(f"  Skipped {skipped} cases without sufficient data")
    
    survival_df = pd.DataFrame(survival_data)
    survival_df = survival_df[survival_df['survival_months'] >= 1].copy()
    
    patients_with_survival = set(survival_df['patient_id'])
    
    print(f"✓ {len(patients_with_survival)} patients with valid survival data")
    print(f"  Deaths: {survival_df['event'].sum()}")
    print(f"  Censored: {len(survival_df) - survival_df['event'].sum()}")
    print(f"  Event rate: {survival_df['event'].mean():.1%}")
    
    # ============================================================
    # STEP 2: Get slides for patients with survival data
    # ============================================================
    print("\n" + "="*60)
    print("STEP 2: Getting slides")
    print("="*60)
    
    files_endpt = 'https://api.gdc.cancer.gov/files'
    
    filters = {
        "op": "and",
        "content": [
            {"op": "=", "content": {"field": "cases.project.project_id", "value": project}},
            {"op": "=", "content": {"field": "files.data_type", "value": "Slide Image"}},
            {"op": "=", "content": {"field": "files.access", "value": "open"}}
        ]
    }
    
    params = {
        "filters": json.dumps(filters),
        "fields": "file_id,file_name,file_size,cases.case_id,cases.submitter_id",
        "format": "JSON",
        "size": "10000"
    }
    
    print(f"Querying all available slides...")
    response = requests.get(files_endpt, params=params)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return None
    
    all_files = response.json()['data']['hits']
    print(f"Found {len(all_files)} total slides")
    
    # Filter to slides with survival data
    filtered_files = []
    
    for file in all_files:
        if not file.get('cases') or len(file['cases']) == 0:
            continue
        
        patient_id = file['cases'][0]['submitter_id']
        
        if patient_id in patients_with_survival:
            filtered_files.append(file)
    
    print(f"✓ {len(filtered_files)} slides have matching survival data")
    
    # ============================================================
    # STEP 3: Keep only one slide per patient (if requested)
    # ============================================================
    if one_slide_per_patient:
        print("\n" + "="*60)
        print("STEP 3: Filtering to one slide per patient")
        print("="*60)
        
        # Try to prefer DX1 (primary diagnostic) slides
        patient_to_slides = {}
        
        for file in filtered_files:
            patient_id = file['cases'][0]['submitter_id']
            
            if patient_id not in patient_to_slides:
                patient_to_slides[patient_id] = []
            
            patient_to_slides[patient_id].append(file)
        
        print(f"Found slides for {len(patient_to_slides)} patients")
        
        # Select one slide per patient
        selected_files = []
        
        for patient_id, slides in patient_to_slides.items():
            # Prefer DX1 slides (primary diagnostic)
            dx1_slides = [s for s in slides if 'DX1' in s['file_name']]
            
            if dx1_slides:
                selected_files.append(dx1_slides[0])
            else:
                # Just take first slide
                selected_files.append(slides[0])
        
        before_count = len(filtered_files)
        filtered_files = selected_files
        
        print(f"Selected {len(filtered_files)} slides (one per patient)")
        print(f"  Filtered out {before_count - len(filtered_files)} duplicate slides")
    
    # Apply n_slides limit if specified
    if n_slides is not None and len(filtered_files) > n_slides:
        filtered_files = filtered_files[:n_slides]
        print(f"✓ Limited to {n_slides} slides as requested")
    
    # ============================================================
    # Create file mapping
    # ============================================================
    file_mapping = []
    for file in filtered_files:
        patient_id = file['cases'][0]['submitter_id']
        case_id = file['cases'][0]['case_id']
        
        file_mapping.append({
            'file_id': file['id'],
            'file_name': file['file_name'],
            'patient_id': patient_id,
            'case_id': case_id,
            'file_size_mb': file['file_size'] / (1024**2)
        })
    
    mapping_df = pd.DataFrame(file_mapping)
    
    # ============================================================
    # Save everything
    # ============================================================
    print("\n" + "="*60)
    print("STEP 4: Saving files")
    print("="*60)
    
    # Manifest
    manifest_path = output_dir / 'gdc_manifest.txt'
    with open(manifest_path, 'w') as f:
        f.write("id\tfilename\tmd5\tsize\tstate\n")
        for file in filtered_files:
            f.write(f"{file['id']}\t{file['file_name']}\t\t{file['file_size']}\tvalidated\n")
    
    print(f"✓ Manifest: {manifest_path}")
    
    # Mappings and survival
    mapping_df.to_csv(output_dir / 'file_to_patient_mapping.csv', index=False)
    survival_df.to_csv(output_dir / 'survival_data.csv', index=False)
    print(f"✓ Mappings and survival data saved")
    
    # Merged dataset
    final_df = mapping_df.merge(survival_df, on='patient_id', how='inner')
    final_df.to_csv(output_dir / 'complete_dataset.csv', index=False)
    
    # ============================================================
    # Summary
    # ============================================================
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    total_size_gb = mapping_df['file_size_mb'].sum() / 1024
    unique_patients = mapping_df['patient_id'].nunique()
    
    print(f"\n✓ Slides in manifest: {len(filtered_files)}")
    print(f"✓ Unique patients: {unique_patients}")
    print(f"✓ Patients with survival: {len(survival_df)}")
    
    if len(filtered_files) == unique_patients:
        print(f"✓ One slide per patient (clean 1:1 mapping)")
    else:
        print(f"⚠  Some patients have multiple slides")
        print(f"   Average: {len(filtered_files)/unique_patients:.2f} slides per patient")
    
    print(f"\nTotal download size: {total_size_gb:.1f} GB")
    
    print(f"\nSurvival statistics:")
    print(f"  Deaths: {final_df['event'].sum()}")
    print(f"  Censored: {(final_df['event']==0).sum()}")
    print(f"  Event rate: {final_df['event'].mean():.1%}")
    print(f"  Median survival: {final_df['survival_months'].median():.1f} months")
    
    # Download script
    slides_dir = output_dir / 'slides'
    download_script = output_dir / 'download_slides.sh'
    
    with open(download_script, 'w') as f:
        f.write("#!/bin/bash\n\n")
        f.write(f"mkdir -p {slides_dir}\n")
        f.write(f"gdc-client download -n 8 -m {manifest_path} -d {slides_dir}\n")
    
    os.chmod(download_script, 0o755)
    
    print("\n" + "="*60)
    print("NEXT STEPS")
    print("="*60)
    print(f"\nDownload slides: {download_script}")
    
    return final_df


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', type=str, default='TCGA-BRCA')
    parser.add_argument('--n_slides', type=int, default=50)
    parser.add_argument('--output_dir', type=str, default=None)
    parser.add_argument('--multiple-slides-per-patient', action='store_true',
                       help='Allow multiple slides per patient (default: one per patient)')
    
    args = parser.parse_args()
    
    if args.output_dir is None:
        args.output_dir = args.project.lower().replace('-', '_') + '_data'
    
    n_slides = None if args.n_slides == -1 else args.n_slides
    
    df = download_tcga_complete(
        project=args.project,
        n_slides=n_slides,
        output_dir=args.output_dir,
        one_slide_per_patient=not args.multiple_slides_per_patient
    )