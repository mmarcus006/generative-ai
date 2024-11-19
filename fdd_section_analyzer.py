from pathlib import Path
import json
import re
from typing import Dict, List, Tuple
from thefuzz import fuzz

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

def main():
    analyzer = FDDSectionAnalyzer("1 TOM PLUMBER GLOBAL, INC._2024.json")
    sections = analyzer.analyze_fdd_sections()
    validation_results = analyzer.validate_sections(sections)
    
    # Print section analysis
    print("\nFDD Section Header Analysis:")
    print("=" * 100)
    print(f"{'Item':<6} {'Page':<6} {'Found':<7} {'Match':<8} Header")
    print("-" * 100)
    
    for result in validation_results:
        status = "✓" if result['found'] else "✗"
        print(f"{result['item_num']:<6} {result['page']:<6} {status:<7} {result['match_type']:<8} {result['text']}")
    
    # Print validation issues
    print("\nValidation Issues:")
    print("=" * 100)
    has_issues = False
    
    for result in validation_results:
        if result['issues']:
            has_issues = True
            print(f"\nItem {result['item_num']} (Page {result['page']}):")
            for issue in result['issues']:
                print(f"  • {issue}")
    
    if not has_issues:
        print("No validation issues found!")
    
    # Summary statistics
    print("\nSummary:")
    print("-" * 50)
    exact_matches = sum(1 for r in validation_results if r['match_type'] == "EXACT")
    fuzzy_matches = sum(1 for r in validation_results if r['match_type'] == "FUZZY")
    missing = sum(1 for r in validation_results if not r['found'])
    total_issues = sum(len(r['issues']) for r in validation_results)
    
    print(f"Exact Matches: {exact_matches}")
    print(f"Fuzzy Matches: {fuzzy_matches}")
    print(f"Missing Items: {missing}")
    print(f"Total Issues Found: {total_issues}")
    
    if missing > 0:
        print("\nMissing Items:")
        for result in validation_results:
            if not result['found']:
                print(f"- Item {result['item_num']}")

if __name__ == "__main__":
    main() 