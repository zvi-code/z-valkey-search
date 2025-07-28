#!/usr/bin/env python3
"""
Quick fuzzy test for integration testing
"""

def test_run_fuzzy_test_suite():
    """Quick fuzzy test that runs fast for integration testing"""
    print("🎲 Quick Fuzzy Memory Estimation Test")
    
    # Import here to avoid module dependency issues during collection
    from integration.unittest.standalone_fuzzy_test import run_fuzzy_test
    
    # Run a very small fuzzy test
    print("Running 3 random patterns...")
    results = run_fuzzy_test(num_patterns=3, seed=42)
    
    # Check basic accuracy
    accuracies = [r['accuracy']['total'] for r in results]
    mean_accuracy = sum(accuracies) / len(accuracies)
    within_range = sum(1 for acc in accuracies if 0.5 <= acc <= 2.0) / len(accuracies)
    
    print(f"✅ Quick test complete: Mean accuracy {mean_accuracy:.2f}x, {within_range:.1%} within range")
    
    # Assert reasonable accuracy
    assert 0.3 <= mean_accuracy <= 3.0, f"Mean accuracy {mean_accuracy:.2f}x outside acceptable range"
    assert within_range >= 0.5, f"Only {within_range:.1%} within reasonable range"
    
    return True

if __name__ == '__main__':
    test_run_fuzzy_test_suite()