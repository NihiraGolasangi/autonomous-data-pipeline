# agents/pipeline_manager_llm.py
"""
LLM-Powered Pipeline Manager
Uses Gemini to make intelligent decisions about data pipeline flow
"""
import sys
import os
from datetime import datetime
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import vertexai
from vertexai.generative_models import GenerativeModel

from agents.ingestion_agent import IngestionAgent
from agents.quality_agent import QualityAgent
from agents.transform_agent import TransformAgent
from agents.loader_agent import LoaderAgent
from utils.config import GCP_PROJECT_ID, LOCATION, QUALITY_THRESHOLD_ABORT, QUALITY_THRESHOLD_CLEAN
from utils.logger import setup_logger

logger = setup_logger(__name__)

class LLMPipelineManager:
    """
    Pipeline Manager with LLM-powered decision making
    Uses Gemini to analyze quality metrics and decide pipeline actions
    """
    
    def __init__(self):
        self.name = "LLMPipelineManager"
        
        # Initialize Vertex AI
        vertexai.init(project=GCP_PROJECT_ID, location=LOCATION)
        self.llm = GenerativeModel("gemini-2.0-flash-001")
        
        # Initialize worker agents
        self.ingestion_agent = IngestionAgent()
        self.quality_agent = QualityAgent()
        self.transform_agent = TransformAgent()
        self.loader_agent = LoaderAgent()
        
        logger.info(f"{self.name} initialized with Gemini LLM for decision-making")
    
    
    def llm_make_decision(self, quality_score, issues, file_name, total_rows):
        """
        Use LLM to decide pipeline action based on quality metrics
        """
        # Prepare issue summary
        issue_summary = []
        for issue in issues:
            if issue.get('type') == 'nulls':
                issue_summary.append(f"- {issue['count']} null values in '{issue['column']}' column")
            elif issue.get('type') == 'duplicates':
                issue_summary.append(f"- {issue['count']} duplicate rows")
            elif issue.get('type') == 'outliers':
                issue_summary.append(f"- {issue['count']} outliers in '{issue['column']}' column (values: {issue.get('values', [])})")
        
        issues_text = "\n".join(issue_summary) if issue_summary else "None"
        
        prompt = f"""You are an expert data pipeline decision-maker analyzing transactional sales/order data.

FILE: {file_name}
TOTAL ROWS: {total_rows}
QUALITY SCORE: {quality_score}/100

ISSUES FOUND:
{issues_text}

Analyze which columns are critical for business operations:
- Transaction fields (order_id, customer_id, amount, dates) are CRITICAL
- Optional fields (comments, notes, feedback, descriptions) are NON-CRITICAL

Make your decision based on:
1. Are issues in critical columns or optional columns?
2. What is the business impact of the issues?
3. Is the data usable despite the issues?

GUIDELINES (not strict rules):
- Score < 60 with critical column issues: likely ABORT
- Score < 60 with only optional column issues: consider PROCEED or CLEAN
- Score 60-80: usually CLEAN, but PROCEED if issues are minor/optional
- Score > 80: usually PROCEED

Use your judgment. Explain your reasoning clearly.

Respond EXACTLY as:
DECISION: [ABORT/CLEAN/PROCEED]
REASONING: [Why, analyzing column criticality and business impact]
CONFIDENCE: [HIGH/MEDIUM/LOW]
"""
        
        try:
            logger.info("Consulting LLM for decision...")
            response = self.llm.generate_content(prompt)
            response_text = response.text.strip()
            
            decision = "CLEAN"
            reasoning = "LLM response parsing failed"
            confidence = "LOW"
            
            for line in response_text.split('\n'):
                if line.startswith('DECISION:'):
                    decision = line.replace('DECISION:', '').strip().upper()
                elif line.startswith('REASONING:'):
                    reasoning = line.replace('REASONING:', '').strip()
                elif line.startswith('CONFIDENCE:'):
                    confidence = line.replace('CONFIDENCE:', '').strip().upper()
            
            logger.info(f"LLM Decision: {decision} (Confidence: {confidence})")
            logger.info(f"LLM Reasoning: {reasoning}")
            
            return {
                "decision": decision,
                "reasoning": reasoning,
                "confidence": confidence,
                "llm_response": response_text
            }
            
        except Exception as e:
            logger.error(f"LLM decision failed: {str(e)}")
            if quality_score < QUALITY_THRESHOLD_ABORT:
                decision = "ABORT"
            elif quality_score < QUALITY_THRESHOLD_CLEAN:
                decision = "CLEAN"
            else:
                decision = "PROCEED"
            
            return {
                "decision": decision,
                "reasoning": f"LLM unavailable, rule-based: score {quality_score}",
                "confidence": "FALLBACK",
                "llm_response": None
            }



    def run_pipeline(self, file_name):
        """
        Run the complete pipeline with LLM decision-making
        
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
            "status": None,
            "llm_powered": True
        }
        
        try:
            # Step 1: Ingestion
            logger.info(f"\n[STEP 1/5] Running Ingestion Agent...")
            ingestion_result = self.ingestion_agent.run(file_name)
            pipeline_state["ingestion"] = ingestion_result
            
            if ingestion_result['status'] != 'success':
                return self._abort_pipeline(pipeline_state, "Ingestion failed")
            
            df = ingestion_result['dataframe']
            logger.info(f"Ingestion complete: {len(df)} rows")
            
            # Step 2: Quality Check
            logger.info(f"\n[STEP 2/5] Running Quality Agent...")
            quality_result = self.quality_agent.run(df)
            pipeline_state["quality"] = quality_result
            
            if quality_result['status'] != 'success':
                return self._abort_pipeline(pipeline_state, "Quality check failed")
            
            quality_score = quality_result['quality_score']
            issues = quality_result['issues']
            logger.info(f"Quality check complete: Score = {quality_score}/100")
            
            # Step 3: LLM Decision
            logger.info(f"\n[STEP 3/5] LLM making intelligent decision...")
            llm_decision = self.llm_make_decision(quality_score, issues, file_name, len(df))
            pipeline_state["llm_decision"] = llm_decision
            
            decision = llm_decision["decision"]
            
            # Step 4: Execute Decision
            if decision == "ABORT":
                logger.warning(f"LLM decided to ABORT pipeline")
                logger.warning(f"   Reasoning: {llm_decision['reasoning']}")
                return self._abort_pipeline(pipeline_state, f"LLM decision: {llm_decision['reasoning']}")
            
            elif decision == "CLEAN":
                logger.info(f"LLM decided to CLEAN data")
                logger.info(f"   Reasoning: {llm_decision['reasoning']}")
                
                logger.info(f"\n[STEP 4/5] Running Transform Agent...")
                transform_result = self.transform_agent.run(df, issues)
                pipeline_state["transform"] = transform_result
                
                if transform_result['status'] != 'success':
                    return self._abort_pipeline(pipeline_state, "Transform failed")
                
                df_to_load = transform_result['dataframe']
                logger.info(f"Transform complete: {transform_result['rows_in']} → {transform_result['rows_out']} rows")
            
            else:  # PROCEED
                logger.info(f"LLM decided to PROCEED without cleaning")
                logger.info(f"   Reasoning: {llm_decision['reasoning']}")
                pipeline_state["transform"] = {
                    "status": "skipped", 
                    "reason": f"LLM decision: {llm_decision['reasoning']}"
                }
                df_to_load = df
            
            # Step 5: Load to BigQuery
            logger.info(f"\n[STEP 5/5] Running Loader Agent...")
            loader_result = self.loader_agent.run(df_to_load)
            pipeline_state["loader"] = loader_result
            
            if loader_result['status'] != 'success':
                return self._abort_pipeline(pipeline_state, "Load failed")
            
            logger.info(f"Load complete: {loader_result['rows_loaded']} rows to {loader_result['destination']}")
            
            # Pipeline Success!
            pipeline_state["status"] = "SUCCESS"
            pipeline_state["end_time"] = datetime.now().isoformat()
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🎉 LLM-POWERED PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"{'='*60}")
            
            return self._generate_report(pipeline_state)
            
        except Exception as e:
            logger.error(f"Pipeline error: {str(e)}")
            return self._abort_pipeline(pipeline_state, str(e))
    
    def _abort_pipeline(self, pipeline_state, reason):
        """Abort pipeline and generate report"""
        pipeline_state["status"] = "ABORTED"
        pipeline_state["abort_reason"] = reason
        pipeline_state["end_time"] = datetime.now().isoformat()
        
        logger.error(f"\n{'='*60}")
        logger.error(f"PIPELINE ABORTED: {reason}")
        logger.error(f"{'='*60}")
        
        return self._generate_report(pipeline_state)
    
    def _generate_report(self, pipeline_state):
        """Generate final pipeline report"""
        report = {
            "pipeline_status": pipeline_state["status"],
            "file_name": pipeline_state["file_name"],
            "llm_powered": pipeline_state.get("llm_powered", True),
            "start_time": pipeline_state["start_time"],
            "end_time": pipeline_state.get("end_time"),
            "stages": {}
        }
        
        # Add each stage result (without dataframe)
        for stage in ["ingestion", "quality", "llm_decision", "transform", "loader"]:
            if stage in pipeline_state:
                stage_data = pipeline_state[stage].copy() if isinstance(pipeline_state[stage], dict) else pipeline_state[stage]
                if isinstance(stage_data, dict):
                    stage_data.pop("dataframe", None)
                report["stages"][stage] = stage_data
        
        # Add abort reason if aborted
        if pipeline_state["status"] == "ABORTED":
            report["abort_reason"] = pipeline_state.get("abort_reason")
        
        logger.info(f" LLM-POWERED PIPELINE REPORT:")
        logger.info(json.dumps(report, indent=2))
        
        return report