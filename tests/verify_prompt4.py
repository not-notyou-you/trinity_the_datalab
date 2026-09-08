#!/usr/bin/env python3
"""
PROMPT 4 VERIFICATION - Simple & Reliable (FINAL)
Save as: tests/verify_prompt4.py
Run: python tests/verify_prompt4.py
"""

import sys
import subprocess
import ast
from pathlib import Path

def check_syntax(file_path):
    """Check Python syntax"""
    try:
        with open(file_path) as f:
            compile(f.read(), file_path, 'exec')
        return True, None
    except SyntaxError as e:
        return False, str(e)

def test_schema_imports():
    """Test schema imports"""
    try:
        from api.schemas import (
            CreateDatasetRequest,
            DatasetSourceConfigResponse,
            DatasetLastConfigResponse
        )
        return True, "All schema classes importable"
    except Exception as e:
        return False, str(e)

def test_pydantic_validation():
    """Test Pydantic validation logic"""
    try:
        from api.schemas import CreateDatasetRequest
        
        # Test 1: Valid request
        valid_request = {
            'location': 'Jabodetabek',
            'date_start': '2024-01-01',
            'date_end': '2024-01-31',
            'name': 'Test Dataset',
            'sources': {
                'sentinel1': {'processing': ['RAW', 'PROCESSED']},
                'modis': {'processing': ['PROCESSED']}
            },
            'fusion_strategy': 'HYBRID',
            'preview_options': ['COLORED']
        }
        
        req = CreateDatasetRequest(**valid_request)
        valid_ok = True
        valid_msg = "Valid request accepted"
        
        # Test 2: Invalid request (missing fusion_strategy with 2 sources)
        invalid_request = valid_request.copy()
        del invalid_request['fusion_strategy']
        
        try:
            req = CreateDatasetRequest(**invalid_request)
            invalid_ok = False
            invalid_msg = "Invalid request accepted (should have rejected)"
        except Exception as e:
            if 'fusion_strategy' in str(e):
                invalid_ok = True
                invalid_msg = "Invalid request correctly rejected"
            else:
                invalid_ok = False
                invalid_msg = f"Wrong rejection reason: {e}"
        
        return valid_ok and invalid_ok, f"{valid_msg}; {invalid_msg}"
    
    except Exception as e:
        return False, f"Import/execution failed: {e}"

def find_route_endpoints():
    """Extract route endpoints from datasets.py"""
    try:
        with open('api/routes/datasets.py') as f:
            content = f.read()
            tree = ast.parse(content)
        
        routes = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if hasattr(node.func, 'attr') and node.func.attr in ['get', 'post', 'delete', 'put']:
                    if node.args and isinstance(node.args[0], ast.Constant):
                        route_path = node.args[0].value
                        method = node.func.attr.upper()
                        routes.append((method, route_path))
        
        return True, routes
    except Exception as e:
        return False, str(e)

def main():
    """Run all verification tests"""
    
    print("=" * 70)
    print("PROMPT 4 VERIFICATION - FINAL")
    print("=" * 70)
    
    checks = {
        'syntax': [],
        'imports': False,
        'validation': False,
        'routes': False,
        'test_files': []
    }
    
    # [1] Syntax Check
    print("\n[1] Python Syntax Check...")
    files_to_check = [
        'api/schemas.py',
        'api/routes/datasets.py',
        'etl/dataset_manager.py'
    ]
    
    for file in files_to_check:
        ok, error = check_syntax(file)
        checks['syntax'].append(ok)
        if ok:
            print(f"    [OK] {file}")
        else:
            print(f"    [FAIL] {file}: {error}")
    
    if not all(checks['syntax']):
        print("\n[CRITICAL] Syntax errors found")
        return False
    
    # [2] Schema Imports
    print("\n[2] Schema Import Check...")
    ok, msg = test_schema_imports()
    checks['imports'] = ok
    if ok:
        print(f"    [OK] {msg}")
    else:
        print(f"    [FAIL] {msg}")
        return False
    
    # [3] Pydantic Validation
    print("\n[3] Pydantic Validation Logic...")
    ok, msg = test_pydantic_validation()
    checks['validation'] = ok
    if ok:
        print(f"    [OK] {msg}")
    else:
        print(f"    [FAIL] {msg}")
    
    # [4] Route Endpoints
    print("\n[4] API Route Registration Check...")
    ok, routes = find_route_endpoints()
    checks['routes'] = ok
    if ok:
        print(f"    [OK] Found {len(routes)} routes")
        
        has_last_config = any('/last-config' in path for _, path in routes)
        has_create = any(path == '' and method == 'POST' for method, path in routes)
        has_get_all = any(path == '' and method == 'GET' for method, path in routes)
        
        print(f"         [OK] GET /datasets/last-config" if has_last_config else "         [FAIL] GET /datasets/last-config")
        print(f"         [OK] POST /datasets" if has_create else "         [WARN] POST /datasets")
        print(f"         [OK] GET /datasets" if has_get_all else "         [WARN] GET /datasets")
    else:
        print(f"    [FAIL] {routes}")
        checks['routes'] = False
    
    # [5] Test Files
    print("\n[5] Test Files Check...")
    test_files = [
        'tests/test_dataset_sources_api.py',
        'tests/test_pipeline_branching.py',
        'tests/test_processing_plan.py'
    ]
    
    for test_file in test_files:
        exists = Path(test_file).exists()
        checks['test_files'].append(exists)
        if exists:
            print(f"    [OK] {test_file}")
        else:
            print(f"    [FAIL] {test_file} NOT FOUND")
    
    # [6] Run pytest (NOT captured — output goes to console)
    print("\n[6] Running pytest (test_dataset_sources_api.py)...")
    print("    " + "=" * 66)
    print("    (pytest output below; this may take 10-30 seconds)")
    print("    " + "=" * 66 + "\n")
    
    # Run pytest directly without capturing — let it output to console
    result = subprocess.run(
        [
            sys.executable, '-m', 'pytest',
            'tests/test_dataset_sources_api.py',
            '-v',
            '--tb=short',
            '--color=auto'
        ]
    )
    
    pytest_ok = (result.returncode == 0)
    
    print("\n    " + "=" * 66)
    
    # Summary
    print("\n" + "=" * 70)
    
    all_checks_ok = (
        all(checks['syntax']) and
        checks['imports'] and
        checks['validation'] and
        checks['routes'] and
        all(checks['test_files']) and
        pytest_ok
    )
    
    if all_checks_ok:
        print("[SUCCESS] All verification checks passed!")
        print("=" * 70)
        print("\nSAFE TO PROCEED TO PROMPT 5")
        print("Next: Rewrite web/app.js Step 2 wizard (per-satellite UI)\n")
        return True
    else:
        print("[WARNING] Some checks failed:")
        print("=" * 70)
        if not pytest_ok:
            print("  • pytest did not pass (see output above)")
        if not checks['imports']:
            print("  • Schema imports failed")
        if not checks['validation']:
            print("  • Pydantic validation failed")
        if not checks['routes']:
            print("  • Route registration failed")
        print("\nFix issues above before proceeding to PROMPT 5\n")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)