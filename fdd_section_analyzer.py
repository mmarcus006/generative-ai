from pathlib import Path
import json
import re
import csv
from typing import Dict, List, Tuple
from thefuzz import fuzz
from datetime import datetime

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
    
    def analyze_fdd_sections(self) -> List[Tuple[int, str, int, bool, str]]:
        """Analyze sections for FDD Items 1-23."""
        sections = []
        item_pattern = re.compile(r'ITEM\s+(\d+)', re.IGNORECASE)
        
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
            match_type = "NOT FOUND"
            
            # First try exact "ITEM X" pattern match
            for header in headers:
                text = header['text']
                match = item_pattern.search(text)
                if match and int(match.group(1)) == item_num:
                    sections.append((item_num, text, header['page_no'], True, "EXACT"))
                    found = True
                    break
            
            # If not found, try fuzzy matching
            if not found:
                for header in headers:
                    text = header['text']
                    if self._fuzzy_match_header(text, item_num):
                        sections.append((item_num, text, header['page_no'], True, "FUZZY"))
                        found = True
                        break
            
            # If still not found, add as missing
            if not found:
                sections.append((item_num, f"NOT FOUND (Expected: ITEM {item_num}: {self.FDD_ITEMS[item_num]})", 
                               0, False, "MISSING"))
        
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
    
    # Lists to store all results for combined analysis
    all_analyses = []
    all_issues = []
    all_summaries = []
    
    # Process each JSON file
    for json_file in input_path.glob('*.json'):
        try:
            print(f"\nProcessing: {json_file.name}")
            analyzer = FDDSectionAnalyzer(str(json_file))
            sections = analyzer.analyze_fdd_sections()
            validation_results = analyzer.validate_sections(sections)
            
            analysis_file, issues_file, summary_file = analyzer.save_results(
                validation_results, output_dir, json_file)
            
            all_analyses.append(analysis_file)
            all_issues.append(issues_file)
            all_summaries.append(summary_file)
            
        except Exception as e:
            print(f"Error processing {json_file.name}: {str(e)}")
    
    # Create combined results file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    combined_file = output_dir / f"combined_analysis_{timestamp}.csv"
    
    with open(combined_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Analysis Summary'])
        writer.writerow(['Total Files Processed', len(all_analyses)])
        writer.writerow([])
        writer.writerow(['Individual Results Files'])
        writer.writerow(['Type', 'File Name', 'Path'])
        
        for a, i, s in zip(all_analyses, all_issues, all_summaries):
            writer.writerows([
                ['Analysis', a.name, str(a)],
                ['Issues', i.name, str(i)],
                ['Summary', s.name, str(s)]
            ])
    
    return combined_file

def main():
    input_dir = r"C:\Projects\NewFranchiseData_Windows\FinalFranchiseData_Windows\data\processed_data\json_documents"
    combined_file = analyze_directory(input_dir)
    
    print(f"\nAnalysis complete!")
    print(f"Combined results saved to: {combined_file}")
    print(f"Individual results are in the 'fdd_analysis_results' directory")

if __name__ == "__main__":
    main() 