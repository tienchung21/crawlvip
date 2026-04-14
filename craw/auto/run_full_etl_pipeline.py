import subprocess
import time
import os

SCRIPTS = [
    "nhatot_step1_mergekhuvuc.py",
    "nhatot_step2_normalize_price.py",
    "nhatot_step3_normalize_size.py",
    "nhatot_step4_normalize_type.py",
    "nhatot_step5_group_median.py",
    "nhatot_step6_normalize_date.py"
]

def run_script(script_name):
    print(f"\n>>>>>>>>>>>>>>>> STARTING {script_name} <<<<<<<<<<<<<<<<")
    start = time.time()
    try:
        # Giả định scripts nằm cùng thư mục
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), script_name)
        
        result = subprocess.run(['python3', script_path], check=True)
        
        if result.returncode == 0:
            print(f">>> SUCCESS: {script_name} finished in {time.time() - start:.2f}s")
            return True
        else:
            print(f">>> FAILED: {script_name} returned code {result.returncode}")
            return False
    except Exception as e:
        print(f">>> ERROR running {script_name}: {e}")
        return False

def main():
    print("=== STARTING FULL ETL PIPELINE FOR NHATOT ===")
    overall_start = time.time()
    
    for script in SCRIPTS:
        success = run_script(script)
        if not success:
            print(f"!!! PIPELINE STOPPED DUE TO ERROR IN {script}")
            break
            
    print(f"\n=== PIPELINE FINISHED IN {time.time() - overall_start:.2f}s ===")

if __name__ == "__main__":
    main()
