import json
import logging
from pathlib import Path
from typing import Dict, Optional, List, Tuple
import google.generativeai as genai
from datetime import datetime
import sqlite3
from tqdm import tqdm
import time

# Import our existing processors
from fdd_info_extractor import FDDInfoExtractor
from fdd_document_organizer import FDDDocumentOrganizer
from fdd_section_analyzer import FDDSectionAnalyzer

class FDDProcessingPipeline:
    def __init__(self, json_path: str, db_path: str = "fdd_documents.db"):
        """Initialize the pipeline with paths and setup logging."""
        self.json_path = Path(json_path)
        self.db_path = Path(db_path)
        
        # Create organized output directories
        self.output_dir = Path('pipeline_output')
        self.structured_data_dir = self.output_dir / 'structured_data'
        self.grouped_sections_dir = self.output_dir / 'grouped_sections'
        self.logs_dir = self.output_dir / 'logs'
        
        # Create all directories
        for dir_path in [self.output_dir, self.structured_data_dir, 
                        self.grouped_sections_dir, self.logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.setup_logging()
        
        # Initialize processors
        self.info_extractor = FDDInfoExtractor(str(self.json_path))
        self.document_organizer = FDDDocumentOrganizer(str(self.db_path))
        self.section_analyzer = FDDSectionAnalyzer(str(self.json_path))
        
    def setup_logging(self):
        """Setup logging configuration."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.logs_dir / f"pipeline_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def validate_fdd_info(self, fdd_info: Dict) -> bool:
        """Validate the extracted FDD information."""
        required_fields = [
            'franchise_name', 'address', 'issuance_date', 'fdd_year',
            'business_description', 'initial_franchise_fee', 'industry'
        ]
        
        # Check required fields exist and are not empty
        for field in required_fields:
            if not fdd_info.get(field):
                self.logger.error(f"Missing or empty required field: {field}")
                return False
        
        # Validate specific fields
        try:
            # Year should be reasonable
            year = int(fdd_info['fdd_year'])
            if not (2000 <= year <= datetime.now().year + 1):
                self.logger.error(f"Invalid FDD year: {year}")
                return False
            
            # Address should have required components
            address = fdd_info['address']
            if not all(k in address for k in ['street', 'city', 'state', 'zip']):
                self.logger.error("Incomplete address information")
                return False
                
            # Industry should have primary and keywords
            industry = fdd_info['industry']
            if not industry.get('primary') or not industry.get('keywords'):
                self.logger.error("Incomplete industry information")
                return False
            
            # Validate keywords is a non-empty array
            if not isinstance(industry['keywords'], list) or len(industry['keywords']) == 0:
                self.logger.error("Industry keywords must be a non-empty array")
                return False
            
            # Parent company can be null or string
            if 'parent_company' in fdd_info and fdd_info['parent_company'] is not None:
                if not isinstance(fdd_info['parent_company'], str):
                    self.logger.error("Parent company must be a string or null")
                    return False
            
        except (ValueError, TypeError) as e:
            self.logger.error(f"Validation error: {str(e)}")
            return False
            
        return True
    
    def extract_and_validate_info(self, max_retries: int = 3) -> Optional[Dict]:
        """Extract FDD info with retry logic."""
        self.logger.info("Starting FDD information extraction")
        
        for attempt in range(max_retries):
            try:
                fdd_info = self.info_extractor.extract_fdd_info()
                
                if self.validate_fdd_info(fdd_info):
                    self.logger.info("FDD information extracted and validated successfully")
                    return fdd_info
                    
                self.logger.warning(f"Validation failed on attempt {attempt + 1}")
                time.sleep(2)  # Wait before retry
                
            except Exception as e:
                self.logger.error(f"Extraction error on attempt {attempt + 1}: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2)
        
        return None
    
    def analyze_and_organize_sections(self) -> Tuple[List, int]:
        """Analyze sections and organize document."""
        self.logger.info("Starting section analysis")
        
        # Analyze sections
        sections = self.section_analyzer.analyze_fdd_sections()
        validation_results = self.section_analyzer.validate_sections(sections)
        
        # Save section analysis
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        analysis_file = self.output_dir / f"section_analysis_{timestamp}.json"
        with open(analysis_file, 'w', encoding='utf-8') as f:
            json.dump({
                'sections': sections,
                'validation': validation_results
            }, f, indent=2)
        
        # Organize document in database
        doc_id = self.document_organizer.process_document(str(self.json_path))
        
        return sections, doc_id
    
    def save_structured_data(self, fdd_info: Dict) -> Path:
        """Save structured data to JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        company_name = self.json_path.stem.split('_')[0]
        output_file = self.structured_data_dir / f"{company_name}_structured_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(fdd_info, f, indent=2)
            
        self.logger.info(f"Structured data saved to: {output_file}")
        return output_file
    
    def save_grouped_sections(self, sections: List, doc_id: int) -> Path:
        """Save grouped sections to JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        company_name = self.json_path.stem.split('_')[0]
        output_file = self.grouped_sections_dir / f"{company_name}_sections_{timestamp}.json"
        
        # Get section content from database
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            grouped_data = {
                'document_id': doc_id,
                'file_name': self.json_path.name,
                'sections': []
            }
            
            for section in sections:
                # First get the section ID from the sections table
                cursor.execute("""
                    SELECT id FROM sections 
                    WHERE document_id = ? AND item_number = ?
                """, (doc_id, section[0]))
                
                section_row = cursor.fetchone()
                if section_row:
                    section_id = section_row[0]
                    
                    # Then get all content for this section
                    cursor.execute("""
                        SELECT text_content, page_number, sequence_order, bbox_top, bbox_left
                        FROM section_content 
                        WHERE section_id = ?
                        ORDER BY page_number, sequence_order
                    """, (section_id,))
                    
                    content = cursor.fetchall()
                    grouped_data['sections'].append({
                        'item_number': section[0],
                        'header': section[1],
                        'page': section[2],
                        'content': [{
                            'text': row[0],
                            'page': row[1],
                            'sequence': row[2],
                            'position': {
                                'top': row[3],
                                'left': row[4]
                            }
                        } for row in content]
                    })
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(grouped_data, f, indent=2)
            
        self.logger.info(f"Grouped sections saved to: {output_file}")
        return output_file
    
    def run_pipeline(self) -> Dict:
        """Run the complete processing pipeline."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results = {
            'timestamp': timestamp,
            'file_name': self.json_path.name,
            'status': 'started'
        }
        
        try:
            # Step 1: Extract and validate FDD information
            self.logger.info("Step 1: Extracting FDD information")
            fdd_info = self.extract_and_validate_info()
            if not fdd_info:
                raise ValueError("Failed to extract valid FDD information")
            
            # Save structured data
            structured_file = self.save_structured_data(fdd_info)
            results['structured_data_path'] = str(structured_file)
            
            # Step 2: Analyze and organize sections
            self.logger.info("Step 2: Analyzing and organizing sections")
            sections, doc_id = self.analyze_and_organize_sections()
            
            # Save grouped sections
            grouped_file = self.save_grouped_sections(sections, doc_id)
            results['grouped_sections_path'] = str(grouped_file)
            
            results.update({
                'status': 'completed',
                'sections_count': len(sections),
                'document_id': doc_id,
                'fdd_info': fdd_info
            })
            
            # Save final results
            results_file = self.output_dir / f"pipeline_results_{timestamp}.json"
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
            
            self.logger.info("Pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            results['status'] = 'failed'
            results['error'] = str(e)
            raise
        
        return results

def main():
    # Process the Tom Plumber FDD
    json_path = "1 TOM PLUMBER GLOBAL, INC._2024.json"
    
    try:
        pipeline = FDDProcessingPipeline(json_path)
        results = pipeline.run_pipeline()
        
        print("\nPipeline Results:")
        print("=" * 50)
        print(f"Status: {results['status']}")
        print(f"Document ID: {results.get('document_id')}")
        print(f"Sections Processed: {results.get('sections_count')}")
        print("\nOutput Files:")
        print(f"Structured Data: {results.get('structured_data_path')}")
        print(f"Grouped Sections: {results.get('grouped_sections_path')}")
        print(f"Pipeline Output Directory: {pipeline.output_dir}")
        
    except Exception as e:
        print(f"\nPipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 