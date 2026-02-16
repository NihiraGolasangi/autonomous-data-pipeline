# agents/pipeline_manager.py
import sys
import os
from datetime import datetime
import json

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.ingestion_agent import IngestionAgent
from agents.quality_agent import QualityAgent
from agents.transform_agent import TransformAgent
from agents.loader_agent import LoaderAgent
from utils.config import QUALITY_THRESHOLD_ABORT, QUALITY_THRESHOLD_CLEAN
from utils.logger import setup_logger

logger = setup_logger(__name__)

class PipelineManager:
    """
    Pipeline Manager: Orchestrates all agents and makes decisions
    """
    
    def __init__(self):
        self.name = "PipelineManager"
        self.ingestion_agent = IngestionAgent()
        self.quality_agent = QualityAgent()
        self.transform_agent = TransformAgent()
        self.loader_agent = LoaderAgent()
        logger.info(f"{self.name} initialized with 4 worker agents")
    
    def run_pipeline(self, file_name):
        """
        Run the complete pipeline for a file
        
        Args:
            file_name: Name of file in GCS bucket
        
        Returns:
            dict with complete pipeline report
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"{self.name} starting pipeline for: {file_name}")
        logger.info(f"{'='*60}")
        
        pipeline_state = {
            "file_name": file_name,
            "start_time": datetime.now().isoformat(),
            "status": None
        }
        
        try:
            # Step 1: Ingestion
            logger.info(f"\n[STEP 1/5] Running Ingestion Agent...")
            ingestion_result = self.ingestion_agent.run(file_name)
            pipeline_state["ingestion"] = ingestion_result
            
            if ingestion_result['status'] != 'success':
                return self._abort_pipeline(pipeline_state, "Ingestion failed")
            
            df = ingestion_result['dataframe']
            logger.info(f"✅ Ingestion complete: {len(df)} rows")
            
            # Step 2: Quality Check
            logger.info(f"\n[STEP 2/5] Running Quality Agent...")
            quality_result = self.quality_agent.run(df)
            pipeline_state["quality"] = quality_result
            
            if quality_result['status'] != 'success':
                return self._abort_pipeline(pipeline_state, "Quality check failed")
            
            quality_score = quality_result['quality_score']
            issues = quality_result['issues']
            logger.info(f"✅ Quality check complete: Score = {quality_score}/100")
            
            # Step 3: Decision Point
            logger.info(f"\n[STEP 3/5] Making decision based on quality score...")
            
            if quality_score < QUALITY_THRESHOLD_ABORT:
                # Score < 60: ABORT
                logger.warning(f"❌ Quality score {quality_score} < {QUALITY_THRESHOLD_ABORT}: ABORTING pipeline")
                return self._abort_pipeline(pipeline_state, f"Quality too low ({quality_score})")
            
            elif quality_score < QUALITY_THRESHOLD_CLEAN:
                # Score 60-80: CLEAN
                logger.info(f"⚠️  Quality score {quality_score} < {QUALITY_THRESHOLD_CLEAN}: CLEANING required")
                
                logger.info(f"\n[STEP 4/5] Running Transform Agent...")
                transform_result = self.transform_agent.run(df, issues)
                pipeline_state["transform"] = transform_result
                
                if transform_result['status'] != 'success':
                    return self._abort_pipeline(pipeline_state, "Transform failed")
                
                df_to_load = transform_result['dataframe']
                logger.info(f"✅ Transform complete: {transform_result['rows_in']} → {transform_result['rows_out']} rows")
            
            else:
                # Score > 80: SKIP TRANSFORM
                logger.info(f"✅ Quality score {quality_score} >= {QUALITY_THRESHOLD_CLEAN}: SKIPPING transform")
                pipeline_state["transform"] = {"status": "skipped", "reason": "Quality score high enough"}
                df_to_load = df
            
            # Step 5: Load to BigQuery
            logger.info(f"\n[STEP 5/5] Running Loader Agent...")
            loader_result = self.loader_agent.run(df_to_load)
            pipeline_state["loader"] = loader_result
            
            if loader_result['status'] != 'success':
                return self._abort_pipeline(pipeline_state, "Load failed")
            
            logger.info(f"✅ Load complete: {loader_result['rows_loaded']} rows to {loader_result['destination']}")
            
            # Pipeline Success!
            pipeline_state["status"] = "SUCCESS"
            pipeline_state["end_time"] = datetime.now().isoformat()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🎉 PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"{'='*60}")
            
            return self._generate_report(pipeline_state)
            
        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")
            return self._abort_pipeline(pipeline_state, str(e))
    
    def _abort_pipeline(self, pipeline_state, reason):
        """
        Abort pipeline and generate report
        """
        pipeline_state["status"] = "ABORTED"
        pipeline_state["abort_reason"] = reason
        pipeline_state["end_time"] = datetime.now().isoformat()
        
        logger.error(f"\n{'='*60}")
        logger.error(f"❌ PIPELINE ABORTED: {reason}")
        logger.error(f"{'='*60}")
        
        return self._generate_report(pipeline_state)
    
    def _generate_report(self, pipeline_state):
        """
        Generate final pipeline report
        """
        report = {
            "pipeline_status": pipeline_state["status"],
            "file_name": pipeline_state["file_name"],
            "start_time": pipeline_state["start_time"],
            "end_time": pipeline_state.get("end_time"),
            "stages": {}
        }
        
        # Add each stage result (without dataframe)
        for stage in ["ingestion", "quality", "transform", "loader"]:
            if stage in pipeline_state:
                stage_data = pipeline_state[stage].copy()
                # Remove dataframe to keep report clean
                stage_data.pop("dataframe", None)
                report["stages"][stage] = stage_data
        
        # Add abort reason if aborted
        if pipeline_state["status"] == "ABORTED":
            report["abort_reason"] = pipeline_state.get("abort_reason")
        
        logger.info(f"\n📊 PIPELINE REPORT:")
        logger.info(json.dumps(report, indent=2))
        
        return report

# Manager → Ingestion → Quality → Decision → Transform (maybe) → Load → Report