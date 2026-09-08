#!/usr/bin/env python3
"""
PROMPT 4 DEBUG - Show exact test failure
Run: python debug_prompt4.py
"""

import subprocess
import sys

def main():
    print("=" * 80)
    print("PYTEST DETAILED OUTPUT - test_dataset_sources_api.py")
    print("=" * 80)
    print()
    
    result = subprocess.run([
        "pytest",
        "tests/test_dataset_sources_api.py",
        "-v",
        "--tb=long",
        "-ra",
    ])
    
    print()
    print("=" * 80)
    if result.returncode == 0:
        print("RESULT: ALL TESTS PASSED")
    else:
        print(f"RESULT: SOME TESTS FAILED (exit code: {result.returncode})")
    print("=" * 80)
    
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())
