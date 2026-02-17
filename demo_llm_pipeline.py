# demo_llm_pipeline.py
"""
Demo script for LLM-Powered Pipeline Manager
Shows how Gemini makes intelligent decisions about data pipeline flow
"""
import os
import json
from agents.pipeline_manager_llm import LLMPipelineManager
from utils.logger import setup_logger

logger = setup_logger(__name__)

def main():
    print("\n" + "="*80)
    print("LLM-POWERED PIPELINE DEMO - Intelligent Decision Making with Gemini")
    print("="*80)
    
    # Test files
    test_files = [
        ("day2_messy.csv", "Quality ~60, has nulls/duplicates/outliers in critical columns"),
        ("day4.csv", "Quality ~67, nulls only in optional 'comments' column")
    ]
    
    llm_manager = LLMPipelineManager()
    results = []
    
    for file_name, description in test_files:
        print(f"\n{'='*80}")
        print(f"Processing: {file_name}")
        print(f"Scenario: {description}")
        print("="*80)
        
        result = llm_manager.run_pipeline(file_name)
        results.append(result)
        
        # Show LLM decision
        if 'llm_decision' in result['stages']:
            llm_decision = result['stages']['llm_decision']
            print(f"\nLLM DECISION SUMMARY:")
            print(f"  Decision: {llm_decision.get('decision')}")
            print(f"  Reasoning: {llm_decision.get('reasoning')}")
            print(f"  Confidence: {llm_decision.get('confidence')}")
        
        print(f"\nPipeline Status: {result['pipeline_status']}")
    
    # Save results
    os.makedirs("reports", exist_ok=True)
    with open("reports/llm_demo_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print("\n" + "="*80)
    print("LLM Pipeline Demo Complete!")
    print("Detailed results saved to: reports/llm_demo_results.json")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()