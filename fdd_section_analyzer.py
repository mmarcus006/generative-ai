from pathlib import Path
import json
import re
import csv
from typing import Dict, List, Tuple
from thefuzz import fuzz
from datetime import datetime
from tqdm import tqdm

class FDDSectionAnalyzer:
    # Standard FDD item titles
    FDD_ITEMS = {
        1: "THE FRANCHISOR, AND ANY PARENTS, PREDECESSORS AND AFFILIATES",
        2: "BUSINESS EXPERIENCE",
        3: "LITIGATION",
        4: "BANKRUPTCY",
        5: "INITIAL FEES",
        6: "OTHER FEES",
        7: "ESTIMATED INITIAL INVESTMENT",
        8: "RESTRICTIONS ON SOURCES OF PRODUCTS AND SERVICES",
        9: "FRANCHISEE'S OBLIGATIONS",
        10: "FINANCING",
        11: "FRANCHISOR'S ASSISTANCE, ADVERTISING, COMPUTER SYSTEMS AND TRAINING",
        12: "TERRITORY",
        13: "TRADEMARKS",
        14: "PATENTS, COPYRIGHTS AND PROPRIETARY INFORMATION",
        15: "OBLIGATION TO PARTICIPATE IN THE ACTUAL OPERATION OF THE FRANCHISE BUSINESS",
        16: "RESTRICTIONS ON WHAT THE FRANCHISEE MAY SELL",
        17: "RENEWAL, TERMINATION, TRANSFER AND DISPUTE RESOLUTION",
        18: "PUBLIC FIGURES",
        19: "FINANCIAL PERFORMANCE REPRESENTATIONS",
        20: "OUTLETS AND FRANCHISEE INFORMATION",
        21: "FINANCIAL STATEMENTS",
        22: "CONTRACTS",
        23: "RECEIPTS"
    }
    
    # Add common variations of item headers
    ITEM_PATTERNS = {
        'standard': r'ITEM\s+(\d+)',
        'section': r'SECTION\s+(\d+)',
        'numbered': r'^\s*(\d+)\.',
        'roman': r'([IVX]+)\.'
    }
    
    # Add known special cases
    SPECIAL_SECTIONS = {
        'table_of_contents': {'keywords': ['contents', 'table of contents']},
        'exhibits': {'keywords': ['exhibit', 'appendix']},
        'index': {'keywords': ['index', 'table of']}
    }
    
    def __init__(self, json_path: str):
        """Initialize analyzer with path to Docling JSON file."""
        self.json_path = Path(json_path)
        self.document = self._load_document()
        
    def _load_document(self) -> Dict:
        """Load and parse the Docling JSON document."""
        with open(self.json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _fuzzy_match_header(self, text: str, item_num: int) -> bool:
        """Check if text matches the standard FDD item title using fuzzy matching."""
        standard_title = f"ITEM {item_num}: {self.FDD_ITEMS[item_num]}"
        # Check both with and without "ITEM X:" prefix
        ratio1 = fuzz.ratio(text.upper(), standard_title.upper())
        ratio2 = fuzz.ratio(text.upper(), self.FDD_ITEMS[item_num].upper())
        
        # Also check partial token matching for better flexibility
        token_ratio = fuzz.partial_token_sort_ratio(text.upper(), standard_title.upper())
        
        return max(ratio1, ratio2, token_ratio) > 80  # Threshold of 80%
    
    def _validate_page_numbers(self, page_no: int, prev_page: int, context: dict) -> List[str]:
        """Enhanced page number validation with context awareness."""
        issues = []
        
        # Ignore validation in special sections
        if context.get('in_special_section'):
            return issues
            
        # Allow small backwards jumps (e.g., for cross-references)
        if page_no < prev_page:
            gap = prev_page - page_no
            if gap > 3:  # Configurable threshold
                issues.append(f"Large non-sequential jump: {gap} pages backward")
        
        # More reasonable threshold for forward gaps
        if prev_page > 0 and (page_no - prev_page) > 30:
            issues.append(f"Large gap: {page_no - prev_page} pages")
            
        return issues
    
    def _detect_item_number(self, text: str, item_num: int) -> Tuple[bool, str]:
        """Enhanced item number detection with multiple patterns."""
        text = text.upper()
        
        # Try all patterns
        for pattern_name, pattern in self.ITEM_PATTERNS.items():
            match = re.search(pattern, text)
            if match:
                found_num = match.group(1)
                # Convert roman numerals if needed
                if pattern_name == 'roman':
                    found_num = self._roman_to_int(found_num)
                if str(found_num) == str(item_num):
                    return True, pattern_name
        
        return False, ''
    
    def _enhanced_fuzzy_match(self, text: str, item_num: int) -> Tuple[bool, float]:
        """Enhanced fuzzy matching with weighted components."""
        standard_title = f"ITEM {item_num}: {self.FDD_ITEMS[item_num]}"
        
        # Weight different aspects of the match
        weights = {
            'full_match': 0.4,
            'content_match': 0.4,
            'number_match': 0.2
        }
        
        scores = {
            'full_match': fuzz.ratio(text.upper(), standard_title.upper()),
            'content_match': fuzz.partial_token_sort_ratio(text.upper(), self.FDD_ITEMS[item_num].upper()),
            'number_match': 100 if str(item_num) in text else 0
        }
        
        weighted_score = sum(scores[k] * weights[k] for k in weights)
        return weighted_score > 75, weighted_score
    
    def validate_sections(self, sections: List[Tuple[int, str, int, bool, str]]) -> List[Dict]:
        """Validate the found sections for common issues."""
        validation_results = []
        
        # Check for sequential page numbers
        prev_page = 0
        for item_num, text, page_no, found, match_type in sections:
            issues = []
            
            # Skip page number validation for missing sections
            if found:
                # Check if page numbers are increasing
                if page_no < prev_page:
                    issues.append(f"Non-sequential page number: Item {item_num} (page {page_no}) comes after page {prev_page}")
                
                # Check for unreasonable page jumps (more than 20 pages)
                if prev_page > 0 and (page_no - prev_page) > 20:
                    issues.append(f"Large page gap: {page_no - prev_page} pages between items")
                
                prev_page = page_no
            
            # Check for missing required items
            if not found and item_num in [1, 2, 3, 4, 5, 6, 7, 21, 22, 23]:  # Critical items
                issues.append(f"Missing critical FDD Item {item_num}")
            
            # Check for proper item format if found
            if found:
                # Check if title contains item number
                if not re.search(rf'\b{item_num}\b', text):
                    issues.append(f"Item number {item_num} not found in header text")
            
            validation_results.append({
                'item_num': item_num,
                'page': page_no,
                'found': found,
                'match_type': match_type,
                'text': text,
                'issues': issues
            })
        
        return validation_results
    
    def _analyze_document_structure(self) -> Dict:
        """Analyze document structure to identify special sections and item locations."""
        structure = {
            'special_sections': [],
            'item_locations': {},
            'context': {'in_special_section': False}
        }
        
        # Analyze all text objects for structure
        for text in self.document['texts']:
            text_content = text.get('text', '').lower()
            
            # Check for special sections
            for section_type, info in self.SPECIAL_SECTIONS.items():
                if any(keyword in text_content for keyword in info['keywords']):
                    structure['special_sections'].append({
                        'type': section_type,
                        'page': text.get('prov', [{}])[0].get('page_no', 0)
                    })
            
            # Look for item headers
            for pattern_name, pattern in self.ITEM_PATTERNS.items():
                match = re.search(pattern, text.get('text', ''), re.IGNORECASE)
                if match:
                    try:
                        item_num = int(match.group(1))
                        if 1 <= item_num <= 23:
                            structure['item_locations'][item_num] = {
                                'page': text.get('prov', [{}])[0].get('page_no', 0),
                                'pattern': pattern_name,
                                'text': text.get('text', '')
                            }
                    except ValueError:
                        continue
        
        return structure
    
    def analyze_fdd_sections(self) -> List[Tuple[int, str, int, bool, str]]:
        """Analyze sections for FDD Items 1-23."""
        sections = []
        document_structure = self._analyze_document_structure()
        
        # First pass: collect all section headers
        headers = []
        for text in self.document['texts']:
            if text.get('label') == 'section_header':
                headers.append({
                    'text': text.get('text', ''),
                    'page_no': text.get('prov', [{}])[0].get('page_no', ''),
                    'self_ref': text.get('self_ref', '')
                })
        
        # Check for each FDD item
        for item_num in range(1, 24):
            found = False
            for header in headers:
                text = header['text']
                
                # Try exact match first
                match = re.search(rf'ITEM\s+{item_num}\b', text, re.IGNORECASE)
                if match:
                    sections.append((item_num, text, header['page_no'], True, "EXACT"))
                    found = True
                    break
                
                # Try fuzzy match if no exact match
                if not found and self._fuzzy_match_header(text, item_num):
                    sections.append((item_num, text, header['page_no'], True, "FUZZY"))
                    found = True
                    break
            
            # If still not found
            if not found:
                sections.append((item_num, "NOT FOUND", 0, False, "MISSING"))
        
        return sections
    
    def save_results(self, validation_results: List[Dict], output_dir: Path, source_file: Path):
        """Save analysis results and validation issues to CSV files."""
        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate timestamp for file names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save section analysis results
        analysis_file = output_dir / f"fdd_analysis_{timestamp}.csv"
        with open(analysis_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['File_Name', 'File_Path', 'Item', 'Page', 'Found', 'Match_Type', 'Header_Text'])
            writer.writeheader()
            for result in validation_results:
                writer.writerow({
                    'File_Name': source_file.name,
                    'File_Path': str(source_file),
                    'Item': result['item_num'],
                    'Page': result['page'],
                    'Found': '✓' if result['found'] else '✗',
                    'Match_Type': result['match_type'],
                    'Header_Text': result['text']
                })
        
        # Save validation issues
        issues_file = output_dir / f"fdd_validation_{timestamp}.csv"
        with open(issues_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['File_Name', 'File_Path', 'Item', 'Page', 'Issue'])
            writer.writeheader()
            for result in validation_results:
                if result['issues']:
                    for issue in result['issues']:
                        writer.writerow({
                            'File_Name': source_file.name,
                            'File_Path': str(source_file),
                            'Item': result['item_num'],
                            'Page': result['page'],
                            'Issue': issue
                        })
        
        # Save summary statistics
        summary_file = output_dir / f"fdd_summary_{timestamp}.csv"
        with open(summary_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['File_Name', 'File_Path', 'Metric', 'Count'])
            
            exact_matches = sum(1 for r in validation_results if r['match_type'] == "EXACT")
            fuzzy_matches = sum(1 for r in validation_results if r['match_type'] == "FUZZY")
            missing = sum(1 for r in validation_results if not r['found'])
            total_issues = sum(len(r['issues']) for r in validation_results)
            
            writer.writerows([
                [source_file.name, str(source_file), 'Exact Matches', exact_matches],
                [source_file.name, str(source_file), 'Fuzzy Matches', fuzzy_matches],
                [source_file.name, str(source_file), 'Missing Items', missing],
                [source_file.name, str(source_file), 'Total Issues', total_issues]
            ])
        
        return analysis_file, issues_file, summary_file

def analyze_directory(input_dir: str):
    """Analyze all JSON files in the specified directory."""
    input_path = Path(input_dir)
    output_dir = Path('fdd_analysis_results')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Collect all JSON files first
    json_files = list(input_path.glob('*.json'))
    
    # Initialize summary data
    summary_data = []
    all_issues = []
    
    # Process each JSON file with progress bar
    for json_file in tqdm(json_files, desc="Processing FDD files"):
        try:
            analyzer = FDDSectionAnalyzer(str(json_file))
            sections = analyzer.analyze_fdd_sections()
            validation_results = analyzer.validate_sections(sections)
            
            # Collect summary statistics
            exact_matches = sum(1 for r in validation_results if r['match_type'] == "EXACT")
            fuzzy_matches = sum(1 for r in validation_results if r['match_type'] == "FUZZY")
            missing = sum(1 for r in validation_results if not r['found'])
            total_issues = sum(len(r['issues']) for r in validation_results)
            
            # Add to summary data
            summary_data.append({
                'File_Name': json_file.name,
                'File_Path': str(json_file),
                'Exact_Matches': exact_matches,
                'Fuzzy_Matches': fuzzy_matches,
                'Missing_Items': missing,
                'Total_Issues': total_issues
            })
            
            # Collect issues
            for result in validation_results:
                if result['issues']:
                    for issue in result['issues']:
                        all_issues.append({
                            'File_Name': json_file.name,
                            'File_Path': str(json_file),
                            'Item': result['item_num'],
                            'Page': result['page'],
                            'Issue': issue
                        })
            
        except Exception as e:
            print(f"\nError processing {json_file.name}: {str(e)}")
    
    # Save directory analysis results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save summary statistics
    summary_file = output_dir / f"directory_summary_{timestamp}.csv"
    with open(summary_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['File_Name', 'File_Path', 'Exact_Matches', 'Fuzzy_Matches', 
                     'Missing_Items', 'Total_Issues']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_data)
    
    # Save all issues
    issues_file = output_dir / f"directory_issues_{timestamp}.csv"
    if all_issues:
        with open(issues_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['File_Name', 'File_Path', 'Item', 'Page', 'Issue']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_issues)
    
    # Calculate overall statistics
    total_files = len(json_files)
    total_exact = sum(d['Exact_Matches'] for d in summary_data)
    total_fuzzy = sum(d['Fuzzy_Matches'] for d in summary_data)
    total_missing = sum(d['Missing_Items'] for d in summary_data)
    total_issues = sum(d['Total_Issues'] for d in summary_data)
    
    # Save overall statistics
    stats_file = output_dir / f"directory_statistics_{timestamp}.csv"
    with open(stats_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows([
            ['Overall Statistics'],
            ['Metric', 'Count', 'Average per File'],
            ['Total Files Processed', total_files, '-'],
            ['Total Exact Matches', total_exact, round(total_exact/total_files, 2)],
            ['Total Fuzzy Matches', total_fuzzy, round(total_fuzzy/total_files, 2)],
            ['Total Missing Items', total_missing, round(total_missing/total_files, 2)],
            ['Total Issues Found', total_issues, round(total_issues/total_files, 2)]
        ])
    
    return summary_file, issues_file, stats_file

def main():
    if len(sys.argv) > 1:
        # Single file analysis
        json_file = Path(sys.argv[1])
        analyzer = FDDSectionAnalyzer(str(json_file))
        sections = analyzer.analyze_fdd_sections()
        validation_results = analyzer.validate_sections(sections)
        
        output_dir = Path('fdd_analysis_results')
        analysis_file, issues_file, summary_file = analyzer.save_results(
            validation_results, output_dir, json_file)
        
        print(f"\nAnalysis complete!")
        print(f"Results saved to 'fdd_analysis_results' directory")
        
    else:
        # Directory analysis
        input_dir = r"C:\Projects\NewFranchiseData_Windows\FinalFranchiseData_Windows\data\processed_data\json_documents"
        summary_file, issues_file, stats_file = analyze_directory(input_dir)
        
        print(f"\nDirectory analysis complete!")
        print(f"Summary saved to: {summary_file.name}")
        print(f"Issues saved to: {issues_file.name}")
        print(f"Statistics saved to: {stats_file.name}")

if __name__ == "__main__":
    import sys
    main() 