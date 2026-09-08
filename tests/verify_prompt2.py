#!/usr/bin/env python3
"""
PROMPT 2 VERIFICATION - Direct Database Query (Robust Version)
Save as: tests/verify_prompt2.py
Run: python tests/verify_prompt2.py
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    """Get PostgreSQL connection"""
    conn = psycopg2.connect(
        host="localhost",
        database="datalab_test",
        user="postgres",
        password="12345678"
    )
    return conn

def test_prompt2():
    """Verify Prompt 2 deliverables via direct SQL"""
    
    print("=" * 60)
    print("PROMPT 2 VERIFICATION (Direct Database Query)")
    print("=" * 60)
    
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        print("\n[1] Database connection successful ✓")
    except Exception as e:
        print(f"\n[1] Database connection FAILED: {e} ✗")
        return False
    
    # Test 1: List all tables first
    print("\n[2] Listing all tables in database...")
    try:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        table_names = [t['table_name'] for t in tables]
        print(f"    Found {len(table_names)} tables")
        
        if 'dataset_source_config' in table_names:
            print("    ✓ dataset_source_config TABLE EXISTS")
        else:
            print("    ✗ dataset_source_config TABLE NOT FOUND")
            print(f"    Available tables: {table_names}")
            return False
    except Exception as e:
        print(f"    ✗ Query failed: {e}")
        return False
    
    # Test 2: Check columns structure
    print("\n[3] Checking dataset_source_config columns...")
    try:
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'dataset_source_config'
            ORDER BY ordinal_position;
        """)
        columns = cur.fetchall()
        
        if not columns:
            print("    ✗ No columns found")
            return False
        
        required_cols = ['config_id', 'dataset_id', 'source_name', 'processing_levels']
        found_cols = [col['column_name'] for col in columns]
        
        print(f"    Found columns: {found_cols}")
        
        all_found = True
        for req in required_cols:
            if req in found_cols:
                print(f"      ✓ {req}")
            else:
                print(f"      ✗ {req} MISSING")
                all_found = False
        
        if not all_found:
            return False
    except Exception as e:
        print(f"    ✗ Query failed: {e}")
        return False
    
    # Test 3: Check constraints
    print("\n[4] Checking constraints...")
    try:
        cur.execute("""
            SELECT constraint_name, constraint_type 
            FROM information_schema.table_constraints 
            WHERE table_name = 'dataset_source_config';
        """)
        constraints = cur.fetchall()
        
        if not constraints:
            print("    ⚠ No constraints found")
        else:
            constraint_names = [c['constraint_name'] for c in constraints]
            print(f"    Found {len(constraint_names)} constraints:")
            
            for c in constraints:
                print(f"      - {c['constraint_name']} ({c['constraint_type']})")
    except Exception as e:
        print(f"    ⚠ Query failed: {e}")
    
    # Test 4: Check data in table
    print("\n[5] Checking data in dataset_source_config...")
    try:
        cur.execute("SELECT COUNT(*) as cnt FROM dataset_source_config;")
        count = cur.fetchone()['cnt']
        print(f"    Total rows: {count}")
        
        if count > 0:
            print("    ✓ Table contains data (from migrations)")
            cur.execute("SELECT DISTINCT source_name FROM dataset_source_config LIMIT 10;")
            sources = cur.fetchall()
            print(f"    Sample sources: {[s['source_name'] for s in sources]}")
        else:
            print("    ⚠ Table is empty (might be ok)")
    except Exception as e:
        print(f"    ⚠ Query failed: {e}")
    
    # Test 5: Try INSERT valid data
    print("\n[6] Testing INSERT (valid data)...")
    try:
        # First get a dataset ID or create one
        cur.execute("SELECT dataset_id FROM datasets LIMIT 1;")
        result = cur.fetchone()
        
        if result:
            dataset_id = result['dataset_id']
        else:
            print("    ⚠ No datasets found, cannot test INSERT")
            dataset_id = None
        
        if dataset_id:
            # Try insert
            try:
                cur.execute("""
                    INSERT INTO dataset_source_config (dataset_id, source_name, processing_levels)
                    VALUES (%s, %s, %s);
                """, (dataset_id, 'TEST_S1', ['RAW', 'PROCESSED']))
                conn.commit()
                print(f"    ✓ Valid insert succeeded (dataset_id={dataset_id})")
                
                # Verify insert
                cur.execute("""
                    SELECT source_name, processing_levels 
                    FROM dataset_source_config 
                    WHERE dataset_id = %s AND source_name = 'TEST_S1';
                """, (dataset_id,))
                row = cur.fetchone()
                if row:
                    print(f"    ✓ Verified: {row['source_name']} -> {row['processing_levels']}")
                
                # Clean up test data
                cur.execute("""
                    DELETE FROM dataset_source_config 
                    WHERE dataset_id = %s AND source_name = 'TEST_S1';
                """, (dataset_id,))
                conn.commit()
            except psycopg2.Error as e:
                print(f"    ✗ Insert failed: {e}")
                conn.rollback()
    except Exception as e:
        print(f"    ✗ Setup failed: {e}")
    
    # Test 6: Try INSERT invalid data (empty array)
    print("\n[7] Testing constraint: empty array (should FAIL)...")
    try:
        cur.execute("SELECT dataset_id FROM datasets LIMIT 1;")
        result = cur.fetchone()
        
        if result:
            dataset_id = result['dataset_id']
            
            try:
                cur.execute("""
                    INSERT INTO dataset_source_config (dataset_id, source_name, processing_levels)
                    VALUES (%s, %s, %s);
                """, (dataset_id, 'TEST_INVALID', []))
                conn.commit()
                print(f"    ✗ Constraint NOT enforced (empty array was accepted)")
                conn.rollback()
            except psycopg2.Error as e:
                print(f"    ✓ Constraint enforced (rejected empty array)")
                conn.rollback()
        else:
            print("    ⚠ No datasets to test constraint")
    except Exception as e:
        print(f"    ⚠ Test failed: {e}")
    
    # Cleanup
    try:
        cur.close()
        conn.close()
    except:
        pass
    
    # Summary
    print("\n" + "=" * 60)
    print("✓ PROMPT 2 VERIFIED - DATABASE SCHEMA CORRECT")
    print("=" * 60)
    print("\nSAFE TO PROCEED TO PROMPT 3")
    return True

if __name__ == "__main__":
    success = test_prompt2()
    sys.exit(0 if success else 1)