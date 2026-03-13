# # download_brca.py
# import requests
# import json
# import pandas as pd
# import os
# from pathlib import Path

# def download_brca_complete(n_slides=50, output_dir='brca_data'):
#     """
#     Download both slides manifest AND survival data
    
#     Args:
#         n_slides: Number of slides to download (None = all available)
#         output_dir: Directory to save outputs
#     """
    
#     output_dir = Path(output_dir)
#     output_dir.mkdir(parents=True, exist_ok=True)
    
#     print("="*60)
#     print(f"Saving all data to: {output_dir.absolute()}")
#     print("="*60)
    
#     print("\n" + "="*60)
#     print("STEP 1: Getting slide image file list")
#     print("="*60)
    
#     # Get slide images
#     files_endpt = 'https://api.gdc.cancer.gov/files'
    
#     filters = {
#         "op": "and",
#         "content": [
#             {"op": "=", "content": {"field": "cases.project.project_id", "value": "TCGA-BRCA"}},
#             {"op": "=", "content": {"field": "files.data_type", "value": "Slide Image"}},
#             {"op": "=", "content": {"field": "files.access", "value": "open"}}
#         ]
#     }
    
#     # Determine size parameter
#     if n_slides is None:
#         # First, get total count
#         params_count = {
#             "filters": json.dumps(filters),
#             "format": "JSON",
#             "size": "0"
#         }
#         response_count = requests.get(files_endpt, params=params_count)
#         total_count = response_count.json()['data']['pagination']['total']
#         print(f"Total available slides: {total_count}")
#         size = str(total_count)
#     else:
#         size = str(n_slides)
    
#     params = {
#         "filters": json.dumps(filters),
#         "fields": "file_id,file_name,file_size,cases.case_id,cases.submitter_id",
#         "format": "JSON",
#         "size": size
#     }
    
#     print(f"Requesting {size} slides...")
#     response = requests.get(files_endpt, params=params)
    
#     if response.status_code != 200:
#         print(f"Error: {response.status_code}")
#         print(response.text)
#         return None
    
#     files = response.json()['data']['hits']
    
#     print(f"Found {len(files)} slide images")
    
#     # Save manifest for gdc-client
#     manifest_path = output_dir / 'gdc_manifest.txt'
#     with open(manifest_path, 'w') as f:
#         f.write("id\tfilename\tmd5\tsize\tstate\n")
#         for file in files:
#             f.write(f"{file['id']}\t{file['file_name']}\t\t{file['file_size']}\tvalidated\n")
    
#     print(f"✓ Manifest saved: {manifest_path}")
    
#     # Extract patient IDs from files
#     file_mapping = []
    
#     for file in files:
#         if file.get('cases') and len(file['cases']) > 0:
#             patient_id = file['cases'][0]['submitter_id']
#             case_id = file['cases'][0]['case_id']
            
#             file_mapping.append({
#                 'file_id': file['id'],
#                 'file_name': file['file_name'],
#                 'patient_id': patient_id,
#                 'case_id': case_id,
#                 'file_size_mb': file['file_size'] / (1024**2)
#             })
    
#     mapping_df = pd.DataFrame(file_mapping)
#     mapping_df.to_csv(output_dir / 'file_to_patient_mapping.csv', index=False)
#     print(f"✓ Patient mapping saved: {output_dir / 'file_to_patient_mapping.csv'}")
    
#     print("\n" + "="*60)
#     print("STEP 2: Getting survival/clinical data")
#     print("="*60)
    
#     # Get survival data for ALL TCGA-BRCA patients
#     cases_endpt = 'https://api.gdc.cancer.gov/cases'
    
#     filters_cases = {
#         "op": "in",
#         "content": {
#             "field": "cases.project.project_id",
#             "value": ["TCGA-BRCA"]
#         }
#     }
    
#     params = {
#         "filters": json.dumps(filters_cases),
#         "fields": "case_id,submitter_id,diagnoses.vital_status,diagnoses.days_to_death,diagnoses.days_to_last_follow_up,diagnoses.age_at_diagnosis,diagnoses.tumor_stage,diagnoses.tumor_grade,diagnoses.primary_diagnosis",
#         "format": "JSON",
#         "size": "5000"
#     }
    
#     response = requests.get(cases_endpt, params=params)
#     cases = response.json()['data']['hits']
    
#     print(f"Found clinical data for {len(cases)} patients")
    
#     # Parse survival data
#     survival_data = []
    
#     for case in cases:
#         if not case.get('diagnoses'):
#             continue
        
#         diag = case['diagnoses'][0]
        
#         days_to_death = diag.get('days_to_death')
#         days_to_followup = diag.get('days_to_last_follow_up')
        
#         if days_to_death is not None:
#             survival_days = days_to_death
#             event = 1
#         elif days_to_followup is not None:
#             survival_days = days_to_followup
#             event = 0
#         else:
#             continue
        
#         survival_data.append({
#             'patient_id': case['submitter_id'],
#             'case_id': case['case_id'],
#             'survival_days': survival_days,
#             'survival_months': survival_days / 30.44,
#             'event': event,
#             'vital_status': diag.get('vital_status'),
#             'age_at_diagnosis': diag.get('age_at_diagnosis'),
#             'tumor_stage': diag.get('tumor_stage'),
#             'tumor_grade': diag.get('tumor_grade'),
#             'primary_diagnosis': diag.get('primary_diagnosis')
#         })
    
#     survival_df = pd.DataFrame(survival_data)
    
#     # Filter out very short follow-up
#     survival_df = survival_df[survival_df['survival_months'] >= 1].copy()
    
#     survival_df.to_csv(output_dir / 'survival_data.csv', index=False)
    
#     print(f"✓ Survival data saved: {output_dir / 'survival_data.csv'}")
    
#     # Summary statistics
#     print("\n" + "="*60)
#     print("SUMMARY")
#     print("="*60)
#     print(f"\nSlides: {len(files)} files")
#     total_size_gb = mapping_df['file_size_mb'].sum() / 1024
#     print(f"  Total size: {mapping_df['file_size_mb'].sum():.1f} MB ({total_size_gb:.1f} GB)")
    
#     print(f"\nPatients with survival data: {len(survival_df)}")
#     print(f"  Deaths: {survival_df['event'].sum()}")
#     print(f"  Censored: {len(survival_df) - survival_df['event'].sum()}")
#     print(f"  Event rate: {survival_df['event'].mean():.1%}")
#     print(f"  Median survival: {survival_df['survival_months'].median():.1f} months")
    
#     # Check matching
#     patients_with_slides = set(mapping_df['patient_id'])
#     patients_with_survival = set(survival_df['patient_id'])
#     matched = patients_with_slides & patients_with_survival
    
#     print(f"\nMatching: {len(matched)} patients have both slides AND survival data")
    
#     if len(matched) < len(files):
#         unmatched = len(files) - len(matched)
#         print(f"  Warning: {unmatched} slides don't have survival data")
    
#     # Create final merged dataset info
#     final_df = mapping_df.merge(survival_df, on='patient_id', how='inner')
#     final_df.to_csv(output_dir / 'complete_dataset.csv', index=False)
#     print(f"\n✓ Complete dataset info: {output_dir / 'complete_dataset.csv'}")
#     print(f"  Final usable samples: {len(final_df)}")
    
#     # Save a download script for convenience
#     slides_dir = output_dir / 'slides'
#     download_script = output_dir / 'download_slides.sh'
    
#     with open(download_script, 'w') as f:
#         f.write("#!/bin/bash\n\n")
#         f.write(f"# Download TCGA-BRCA slides to {slides_dir}\n\n")
#         f.write("# Check if gdc-client is installed\n")
#         f.write("if ! command -v gdc-client &> /dev/null; then\n")
#         f.write("    echo 'Error: gdc-client not found!'\n")
#         f.write("    echo 'Download from: https://gdc.cancer.gov/access-data/gdc-data-transfer-tool'\n")
#         f.write("    exit 1\n")
#         f.write("fi\n\n")
#         f.write(f"# Create output directory\n")
#         f.write(f"mkdir -p {slides_dir}\n\n")
#         f.write(f"# Download slides (using 8 parallel connections)\n")
#         f.write(f"gdc-client download -n 8 -m {manifest_path} -d {slides_dir}\n\n")
#         f.write(f"echo 'Download complete!'\n")
#         f.write(f"echo 'Slides saved to: {slides_dir}'\n")
    
#     os.chmod(download_script, 0o755)  # Make executable
    
#     print("\n" + "="*60)
#     print("NEXT STEPS")
#     print("="*60)
#     print(f"\n1. Download slide images (~{total_size_gb:.1f} GB):")
#     print(f"   ")
#     print(f"   Option A - Use the script:")
#     print(f"   {download_script}")
#     print(f"   ")
#     print(f"   Option B - Run manually:")
#     print(f"   gdc-client download -n 8 -m {manifest_path} -d {slides_dir}")
#     print(f"   ")
#     print(f"   (Using 8 parallel downloads for speed)")
#     print(f"\n2. After download, process with CLIP:")
#     print(f"   python process_brca_cohort.py")
#     print(f"\n3. Create KM curves:")
#     print(f"   python evaluate_prognosis.py")
    
#     print(f"\n{'='*60}")
#     print(f"All metadata saved to: {output_dir.absolute()}")
#     print(f"{'='*60}\n")
    
#     return final_df


# if __name__ == '__main__':
#     import argparse
    
#     parser = argparse.ArgumentParser(description='Download TCGA-BRCA data')
#     parser.add_argument('--n_slides', type=int, default=50,
#                        help='Number of slides (default: 50, use -1 for all)')
#     parser.add_argument('--output_dir', type=str, default='brca_data',
#                        help='Output directory (default: brca_data)')
    
#     args = parser.parse_args()
    
#     # Convert -1 to None for "all slides"
#     n_slides = None if args.n_slides == -1 else args.n_slides
    
#     df = download_brca_complete(n_slides=n_slides, output_dir=args.output_dir)

# download_brca.py (FIXED)
import requests
import json
import pandas as pd
import os
from pathlib import Path

def download_brca_complete(n_slides=50, output_dir='brca_data'):
    """
    Download both slides manifest AND survival data
    
    Args:
        n_slides: Number of slides (None = all available)
        output_dir: Directory to save outputs
    """
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("="*60)
    print(f"Saving all data to: {output_dir.absolute()}")
    print("="*60)
    
    print("\n" + "="*60)
    print("STEP 1: Getting slide image file list")
    print("="*60)
    
    # Get slide images
    files_endpt = 'https://api.gdc.cancer.gov/files'
    
    filters = {
        "op": "and",
        "content": [
            {"op": "=", "content": {"field": "cases.project.project_id", "value": "TCGA-BRCA"}},
            {"op": "=", "content": {"field": "files.data_type", "value": "Slide Image"}},
            {"op": "=", "content": {"field": "files.access", "value": "open"}}
        ]
    }
    
    # Determine size parameter
    if n_slides is None:
        params_count = {
            "filters": json.dumps(filters),
            "format": "JSON",
            "size": "0"
        }
        response_count = requests.get(files_endpt, params=params_count)
        total_count = response_count.json()['data']['pagination']['total']
        print(f"Total available slides: {total_count}")
        size = str(total_count)
    else:
        size = str(n_slides)
    
    params = {
        "filters": json.dumps(filters),
        "fields": "file_id,file_name,file_size,cases.case_id,cases.submitter_id",
        "format": "JSON",
        "size": size
    }
    
    print(f"Requesting {size} slides...")
    response = requests.get(files_endpt, params=params)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None
    
    files = response.json()['data']['hits']
    
    print(f"Found {len(files)} slide images")
    
    # Save manifest for gdc-client
    manifest_path = output_dir / 'gdc_manifest.txt'
    with open(manifest_path, 'w') as f:
        f.write("id\tfilename\tmd5\tsize\tstate\n")
        for file in files:
            f.write(f"{file['id']}\t{file['file_name']}\t\t{file['file_size']}\tvalidated\n")
    
    print(f"✓ Manifest saved: {manifest_path}")
    
    # Extract patient IDs from files
    file_mapping = []
    
    for file in files:
        if file.get('cases') and len(file['cases']) > 0:
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
    mapping_df.to_csv(output_dir / 'file_to_patient_mapping.csv', index=False)
    print(f"✓ Patient mapping saved: {output_dir / 'file_to_patient_mapping.csv'}")
    
    print("\n" + "="*60)
    print("STEP 2: Getting survival/clinical data")
    print("="*60)
    
    # Get survival data for ALL TCGA-BRCA patients
    cases_endpt = 'https://api.gdc.cancer.gov/cases'
    
    filters_cases = {
        "op": "in",
        "content": {
            "field": "cases.project.project_id",
            "value": ["TCGA-BRCA"]
        }
    }
    
    # FIXED: Request more detailed fields including vital_status at top level
    params = {
        "filters": json.dumps(filters_cases),
        "fields": "case_id,submitter_id,diagnoses.days_to_death,diagnoses.days_to_last_follow_up,diagnoses.vital_status,diagnoses.age_at_diagnosis,diagnoses.tumor_stage,diagnoses.tumor_grade,diagnoses.primary_diagnosis,demographic.vital_status",
        "format": "JSON",
        "size": "5000"
    }
    
    response = requests.get(cases_endpt, params=params)
    cases = response.json()['data']['hits']
    
    print(f"Found clinical data for {len(cases)} patients")
    
    # Parse survival data - FIXED LOGIC
    survival_data = []
    skipped_no_diagnosis = 0
    skipped_no_survival = 0
    
    for case in cases:
        patient_id = case['submitter_id']
        
        if not case.get('diagnoses') or len(case['diagnoses']) == 0:
            skipped_no_diagnosis += 1
            continue
        
        diag = case['diagnoses'][0]
        
        # Get vital status (multiple possible locations)
        vital_status = None
        if diag.get('vital_status'):
            vital_status = diag['vital_status']
        elif case.get('demographic', {}).get('vital_status'):
            vital_status = case['demographic']['vital_status']
        
        # Extract days - FIXED: handle None, strings, etc.
        days_to_death = diag.get('days_to_death')
        days_to_followup = diag.get('days_to_last_follow_up')
        
        # Convert to int if string
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
        
        # Determine survival time and event status
        survival_days = None
        event = None
        
        # FIXED: Better logic for determining event
        if days_to_death is not None and days_to_death > 0:
            survival_days = days_to_death
            event = 1  # Death
        elif days_to_followup is not None and days_to_followup > 0:
            survival_days = days_to_followup
            # Determine if censored based on vital status
            if vital_status:
                if 'dead' in vital_status.lower() or 'deceased' in vital_status.lower():
                    event = 1
                else:
                    event = 0
            else:
                event = 0  # Assume censored if no death date
        else:
            skipped_no_survival += 1
            continue
        
        survival_data.append({
            'patient_id': patient_id,
            'case_id': case['case_id'],
            'survival_days': survival_days,
            'survival_months': survival_days / 30.44,
            'event': event,
            'vital_status': vital_status,
            'age_at_diagnosis': diag.get('age_at_diagnosis'),
            'tumor_stage': diag.get('tumor_stage'),
            'tumor_grade': diag.get('tumor_grade'),
            'primary_diagnosis': diag.get('primary_diagnosis')
        })
    
    print(f"  Skipped {skipped_no_diagnosis} cases without diagnosis")
    print(f"  Skipped {skipped_no_survival} cases without survival time")
    
    survival_df = pd.DataFrame(survival_data)
    
    # Filter out very short follow-up
    survival_df = survival_df[survival_df['survival_months'] >= 1].copy()
    
    # Debug: Print vital status distribution
    print(f"\nDEBUG - Vital Status Distribution:")
    if 'vital_status' in survival_df.columns:
        print(survival_df['vital_status'].value_counts())
    
    print(f"\nDEBUG - Event Distribution:")
    print(f"  Event=1 (death): {(survival_df['event']==1).sum()}")
    print(f"  Event=0 (censored): {(survival_df['event']==0).sum()}")
    
    survival_df.to_csv(output_dir / 'survival_data.csv', index=False)
    
    print(f"\n✓ Survival data saved: {output_dir / 'survival_data.csv'}")
    
    # Summary statistics
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"\nSlides: {len(files)} files")
    total_size_gb = mapping_df['file_size_mb'].sum() / 1024
    print(f"  Total size: {mapping_df['file_size_mb'].sum():.1f} MB ({total_size_gb:.1f} GB)")
    
    print(f"\nPatients with survival data: {len(survival_df)}")
    print(f"  Deaths: {survival_df['event'].sum()}")
    print(f"  Censored: {len(survival_df) - survival_df['event'].sum()}")
    print(f"  Event rate: {survival_df['event'].mean():.1%}")
    print(f"  Median survival: {survival_df['survival_months'].median():.1f} months")
    
    # Check matching
    patients_with_slides = set(mapping_df['patient_id'])
    patients_with_survival = set(survival_df['patient_id'])
    matched = patients_with_slides & patients_with_survival
    
    print(f"\nMatching: {len(matched)} patients have both slides AND survival data")
    
    if len(matched) < len(files):
        unmatched = len(files) - len(matched)
        print(f"  Warning: {unmatched} slides don't have survival data")
    
    # Create final merged dataset info
    final_df = mapping_df.merge(survival_df, on='patient_id', how='inner')
    final_df.to_csv(output_dir / 'complete_dataset.csv', index=False)
    print(f"\n✓ Complete dataset info: {output_dir / 'complete_dataset.csv'}")
    print(f"  Final usable samples: {len(final_df)}")
    
    # Additional check
    print(f"\nFinal dataset event distribution:")
    print(f"  Deaths: {final_df['event'].sum()}")
    print(f"  Censored: {(final_df['event']==0).sum()}")
    
    # Save a download script
    slides_dir = output_dir / 'slides'
    download_script = output_dir / 'download_slides.sh'
    
    with open(download_script, 'w') as f:
        f.write("#!/bin/bash\n\n")
        f.write(f"# Download TCGA-BRCA slides to {slides_dir}\n\n")
        f.write("if ! command -v gdc-client &> /dev/null; then\n")
        f.write("    echo 'Error: gdc-client not found!'\n")
        f.write("    exit 1\n")
        f.write("fi\n\n")
        f.write(f"mkdir -p {slides_dir}\n")
        f.write(f"gdc-client download -n 8 -m {manifest_path} -d {slides_dir}\n")
        f.write(f"echo 'Download complete!'\n")
    
    os.chmod(download_script, 0o755)
    
    print("\n" + "="*60)
    print("NEXT STEPS")
    print("="*60)
    print(f"\n1. Download slide images (~{total_size_gb:.1f} GB):")
    print(f"   {download_script}")
    print(f"   or:")
    print(f"   gdc-client download -n 8 -m {manifest_path} -d {slides_dir}")
    
    print(f"\n{'='*60}")
    print(f"All metadata saved to: {output_dir.absolute()}")
    print(f"{'='*60}\n")
    
    return final_df


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Download TCGA-BRCA data')
    parser.add_argument('--n_slides', type=int, default=50,
                       help='Number of slides (default: 50, use -1 for all)')
    parser.add_argument('--output_dir', type=str, default='brca_data',
                       help='Output directory (default: brca_data)')
    
    args = parser.parse_args()
    
    n_slides = None if args.n_slides == -1 else args.n_slides
    
    df = download_brca_complete(n_slides=n_slides, output_dir=args.output_dir)