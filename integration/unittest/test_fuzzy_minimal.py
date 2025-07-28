#!/usr/bin/env python3
"""
Minimal fuzzy test that definitely shows output and runs fast
"""

def test_run_fuzzy_test_suite():
    """Minimal fuzzy test that runs very fast"""
    import sys
    
    def force_print(msg):
        print(f"FUZZY_TEST: {msg}", flush=True)
        sys.stdout.flush()
        sys.stderr.flush()
    
    force_print("🎲 Starting MINIMAL fuzzy memory estimation test")
    
    try:
        # Import the calculation function
        force_print("📦 Importing memory calculation function...")
        from integration.unittest.standalone_fuzzy_test import calculate_comprehensive_memory
        force_print("✅ Import successful")
        
        # Test a single simple scenario
        force_print("🧪 Testing simple scenario: 1000 keys, no sharing...")
        
        result = calculate_comprehensive_memory(
            total_keys=1000,
            unique_tags=1000,  # No sharing
            avg_tag_length=20,
            avg_tags_per_key=1,
            avg_keys_per_tag=1,
            vector_dims=8,
            hnsw_m=16
        )
        
        force_print(f"📊 Estimation complete: {result['total_estimated_kb']} KB total")
        force_print(f"   • Tag index: {result['tag_index_kb']} KB")
        force_print(f"   • Vector index: {result['vector_index_kb']} KB")
        
        # Simple validation
        expected_range = (50, 2000)  # KB
        total_kb = result['total_estimated_kb']
        
        force_print(f"🎯 Validating result: {total_kb} KB should be in range {expected_range}")
        
        assert expected_range[0] <= total_kb <= expected_range[1], \
            f"Total memory {total_kb} KB outside expected range {expected_range}"
        
        force_print("✅ FUZZY TEST PASSED - Estimation within expected range")
        
    except Exception as e:
        force_print(f"❌ FUZZY TEST FAILED: {str(e)}")
        import traceback
        force_print(f"Traceback: {traceback.format_exc()}")
        raise
    
    force_print("🏁 Minimal fuzzy test completed successfully")

if __name__ == '__main__':
    test_run_fuzzy_test_suite()