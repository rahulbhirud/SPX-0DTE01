#!/usr/bin/env python3
"""
Simple test runner to show test results and logging output clearly.
"""

import sys
import logging
import subprocess

# Configure logging to show test progress
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    print("=" * 60)
    print("RUNNING SPX CREDIT SPREAD TESTS")
    print("=" * 60)
    
    try:
        # Run pytest with verbose output and logging
        result = subprocess.run([
            sys.executable, '-m', 'pytest', 
            'testcases/test_credit_spreads.py', 
            '-v', '-s', '--tb=short',
            '--log-cli-level=INFO'
        ], capture_output=False, text=True)
        
        print("\n" + "=" * 60)
        if result.returncode == 0:
            print("✅ ALL TESTS PASSED SUCCESSFULLY!")
        else:
            print(f"❌ SOME TESTS FAILED (exit code: {result.returncode})")
        print("=" * 60)
        
        return result.returncode
        
    except Exception as e:
        print(f"❌ ERROR RUNNING TESTS: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())