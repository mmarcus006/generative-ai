import unittest
import json
import os
from pathlib import Path
import sqlite3
from datetime import datetime
from config import GEMINI_API_KEY

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
        
        # Set up test database
        cls.test_db = cls.test_dir / "test_fdd.db"
        if cls.test_db.exists():
            cls.test_db.unlink()
            
    def setUp(self):
        """Set up each test."""
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Clear database before each test
        if self.test_db.exists():
            self.test_db.unlink()
        
    def test_info_extractor(self):
        """Test FDD Info Extractor functionality."""
        try:
            # Initialize extractor with config API key
            extractor = FDDInfoExtractor(str(self.test_file))
            
            # Test text extraction
            text_content = extractor.extract_first_20_pages()
            self.assertIsNotNone(text_content)
            self.assertIsInstance(text_content, str)
            self.assertGreater(len(text_content), 0)
            
            # Save text sample for inspection
            text_sample_file = self.results_dir / f"text_sample_{self.timestamp}.txt"
            with open(text_sample_file, 'w', encoding='utf-8') as f:
                f.write(text_content[:2000])  # First 2000 chars
            
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
            with sqlite3.connect(str(self.test_db)) as conn:
                cursor = conn.cursor()
                
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
                
                # Check section content
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM section_content sc 
                    JOIN sections s ON sc.section_id = s.id 
                    WHERE s.document_id = ?
                """, (doc_id,))
                content_count = cursor.fetchone()[0]
                self.assertGreater(content_count, 0)
                
                # Save database query results
                result_file = self.results_dir / f"document_organizer_results_{self.timestamp}.json"
                results = {
                    'document': list(doc_record),  # Convert tuple to list for JSON serialization
                    'section_count': section_count,
                    'content_count': content_count,
                    'sample_sections': []
                }
                
                # Get sample of sections with their content
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
    
    def test_section_analyzer(self):
        """Test FDD Section Analyzer functionality."""
        try:
            # Initialize analyzer
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
            self.assertEqual(len(sections), 23)  # Should find all 23 items
            found_sections = [s for s in sections if s[3]]  # s[3] is the 'found' flag
            self.assertGreater(len(found_sections), 0)
            
        except Exception as e:
            self.fail(f"Section analysis failed: {str(e)}")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Keep results directory but clean up test data
        if cls.test_db.exists():
            cls.test_db.unlink()

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