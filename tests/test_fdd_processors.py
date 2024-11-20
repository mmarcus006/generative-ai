import unittest
import json
import os
from pathlib import Path
import sqlite3
from datetime import datetime
from config import GEMINI_API_KEY
import time

# Import our modules
from fdd_info_extractor import FDDInfoExtractor
from fdd_document_organizer import FDDDocumentOrganizer
from fdd_section_analyzer import FDDSectionAnalyzer

class TestFDDProcessors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Create test directories
        cls.test_dir = Path("tests/test_data")
        cls.test_dir.mkdir(parents=True, exist_ok=True)
        cls.results_dir = Path("tests/test_results")
        cls.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Use actual Tom Plumber JSON file
        cls.test_file = Path(__file__).parent.parent / "1 TOM PLUMBER GLOBAL, INC._2024.json"
        if not cls.test_file.exists():
            raise FileNotFoundError(f"Test file not found: {cls.test_file}")
            
    def setUp(self):
        """Set up each test."""
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create unique test database for each test
        self.test_db = self.test_dir / f"test_fdd_{self.timestamp}.db"
        
    def tearDown(self):
        """Clean up after each test."""
        # Close any open database connections
        if hasattr(self, 'conn'):
            self.conn.close()
            
        # Remove test database with retry
        if self.test_db.exists():
            max_retries = 3
            for i in range(max_retries):
                try:
                    self.test_db.unlink()
                    break
                except PermissionError:
                    if i == max_retries - 1:
                        print(f"Warning: Could not delete {self.test_db}")
                    time.sleep(1)  # Wait before retry
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Clean up test directories if empty
        try:
            cls.test_dir.rmdir()
            cls.results_dir.rmdir()
        except:
            pass  # Ignore if directories not empty

    def test_info_extractor(self):
        """Test FDD Info Extractor functionality."""
        try:
            extractor = FDDInfoExtractor(str(self.test_file))
            
            # Test text extraction
            text_content = extractor.extract_first_20_pages()
            self.assertIsNotNone(text_content)
            self.assertIsInstance(text_content, str)
            self.assertGreater(len(text_content), 0)
            
            # Save text sample
            text_sample_file = self.results_dir / f"text_sample_{self.timestamp}.txt"
            with open(text_sample_file, 'w', encoding='utf-8') as f:
                f.write(text_content[:2000])
            
            # Test FDD info extraction
            fdd_info = extractor.extract_fdd_info()
            self.assertIsNotNone(fdd_info)
            self.assertIsInstance(fdd_info, dict)
            
            # Save results
            result_file = self.results_dir / f"info_extractor_results_{self.timestamp}.json"
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'text_sample': text_content[:1000],
                    'extracted_info': fdd_info
                }, f, indent=2)
            
            # Validate required fields
            required_fields = ['franchise_name', 'address', 'issuance_date', 'fdd_year']
            for field in required_fields:
                self.assertIn(field, fdd_info)
                
        except Exception as e:
            self.fail(f"Info extraction failed: {str(e)}")
            
    def test_document_organizer(self):
        """Test FDD Document Organizer functionality."""
        try:
            # Initialize organizer with test database
            organizer = FDDDocumentOrganizer(str(self.test_db))
            
            # Process test document
            doc_id = organizer.process_document(str(self.test_file))
            self.assertIsNotNone(doc_id)
            
            # Query and validate database contents
            self.conn = sqlite3.connect(str(self.test_db))
            cursor = self.conn.cursor()
            
            # Check documents table
            cursor.execute("SELECT * FROM documents WHERE id = ?", (doc_id,))
            doc_record = cursor.fetchone()
            self.assertIsNotNone(doc_record)
            
            # Check sections table
            cursor.execute("""
                SELECT COUNT(DISTINCT item_number) 
                FROM sections 
                WHERE document_id = ?
            """, (doc_id,))
            section_count = cursor.fetchone()[0]
            self.assertGreater(section_count, 0)
            
            # Save results
            result_file = self.results_dir / f"document_organizer_results_{self.timestamp}.json"
            results = {
                'document': list(doc_record),
                'section_count': section_count,
                'sample_sections': []
            }
            
            # Get sample sections
            cursor.execute("""
                SELECT s.item_number, s.header_text, 
                       GROUP_CONCAT(SUBSTR(sc.text_content, 1, 200)) as content
                FROM sections s
                LEFT JOIN section_content sc ON s.id = sc.section_id
                WHERE s.document_id = ?
                GROUP BY s.id
                LIMIT 5
            """, (doc_id,))
            results['sample_sections'] = [list(row) for row in cursor.fetchall()]
            
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
                
        except Exception as e:
            self.fail(f"Document organization failed: {str(e)}")
        finally:
            if hasattr(self, 'conn'):
                self.conn.close()
    
    def test_section_analyzer(self):
        """Test FDD Section Analyzer functionality."""
        try:
            analyzer = FDDSectionAnalyzer(str(self.test_file))
            
            # Test section analysis
            sections = analyzer.analyze_fdd_sections()
            self.assertIsNotNone(sections)
            self.assertIsInstance(sections, list)
            
            # Test validation
            validation_results = analyzer.validate_sections(sections)
            self.assertIsNotNone(validation_results)
            
            # Save results
            result_file = self.results_dir / f"section_analyzer_results_{self.timestamp}.json"
            results = {
                'sections': [
                    {
                        'item_num': s[0],
                        'text': s[1],
                        'page': s[2],
                        'found': s[3],
                        'match_type': s[4]
                    }
                    for s in sections
                ],
                'validation': validation_results
            }
            
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
            
            # Validate results
            self.assertEqual(len(sections), 23)
            found_sections = [s for s in sections if s[3]]
            self.assertGreater(len(found_sections), 0)
            
        except Exception as e:
            self.fail(f"Section analysis failed: {str(e)}")

def run_tests():
    """Run tests and generate report."""
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFDDProcessors)
    
    # Run tests and capture results
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Generate test report
    report_file = Path("tests/test_results") / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_file, 'w') as f:
        f.write("FDD Processors Test Report\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Tests Run: {result.testsRun}\n")
        f.write(f"Failures: {len(result.failures)}\n")
        f.write(f"Errors: {len(result.errors)}\n")
        f.write(f"Skipped: {len(result.skipped)}\n\n")
        
        if result.failures:
            f.write("Failures:\n")
            for failure in result.failures:
                f.write(f"{failure[0]}: {failure[1]}\n")
        
        if result.errors:
            f.write("\nErrors:\n")
            for error in result.errors:
                f.write(f"{error[0]}: {error[1]}\n")

if __name__ == '__main__':
    run_tests() 