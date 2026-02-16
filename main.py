# main.py
import os
import json
from datetime import datetime
from agents.pipeline_manager import PipelineManager
from utils.logger import setup_logger

# Set up logging
logger = setup_logger(__name__, log_file="logs/pipeline.log")

def main():
    """
    Main entry point for the autonomous data pipeline
    """
    logger.info("\n" + "="*80)
    logger.info("AUTONOMOUS DATA PIPELINE - STARTING")
    logger.info("="*80)
    
    # Initialize Pipeline Manager
    manager = PipelineManager()
    
    # Test files to process
    test_files = [
        "day1_clean.csv",
        "day2_messy.csv",
        "day3_schema_change.csv"
    ]
    
    # Store all reports
    all_reports = []
    
    # Process each file
    for file_name in test_files:
        logger.info(f"\n\n{'#'*80}")
        logger.info(f"# Processing: {file_name}")
        logger.info(f"{'#'*80}\n")
        
        # Run pipeline
        report = manager.run_pipeline(file_name)
        all_reports.append(report)
        
        # Save individual report
        report_dir = "reports"
        os.makedirs(report_dir, exist_ok=True)
        
        report_file = os.path.join(report_dir, f"{file_name.replace('.csv', '')}_report.json")
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"\n📄 Report saved to: {report_file}")
    
    # Generate summary report
    summary = {
        "total_files": len(test_files),
        "successful": sum(1 for r in all_reports if r["pipeline_status"] == "SUCCESS"),
        "aborted": sum(1 for r in all_reports if r["pipeline_status"] == "ABORTED"),
        "timestamp": datetime.now().isoformat(),
        "files": all_reports
    }
    
    summary_file = os.path.join("reports", "pipeline_summary.json")
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"\n\n{'='*80}")
    logger.info("PIPELINE EXECUTION COMPLETE")
    logger.info(f"{'='*80}")
    logger.info(f"Total files processed: {summary['total_files']}")
    logger.info(f"Successful: {summary['successful']}")
    logger.info(f"Aborted: {summary['aborted']}")
    logger.info(f"\n📊 Summary report saved to: {summary_file}")
    logger.info(f"{'='*80}\n")

if __name__ == "__main__":
    main()