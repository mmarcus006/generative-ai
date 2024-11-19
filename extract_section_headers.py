from pathlib import Path
import json
import csv
from typing import Dict, List

class SectionHeaderExtractor:
    def __init__(self, json_path: str):
        """Initialize extractor with path to Docling JSON file."""
        self.json_path = Path(json_path)
        self.document = self._load_document()
        
    def _load_document(self) -> Dict:
        """Load and parse the Docling JSON document."""
        with open(self.json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def extract_section_headers(self) -> List[Dict]:
        """Extract all text objects with label 'section_header'."""
        headers = []
        
        for text in self.document['texts']:
            if text.get('label') == 'section_header':
                # Create a flattened version of the header
                header = {
                    'self_ref': text.get('self_ref', ''),
                    'text': text.get('text', ''),
                    'level': text.get('level', ''),
                    'page_no': text.get('prov', [{}])[0].get('page_no', ''),
                    'orig': text.get('orig', '')
                }
                headers.append(header)
                
        return headers
    
    def save_to_json(self, headers: List[Dict], output_path: str):
        """Save the extracted headers to a JSON file."""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(headers, f, indent=2)
            
    def save_to_csv(self, headers: List[Dict], output_path: str):
        """Save the extracted headers to a CSV file."""
        if not headers:
            return
            
        # Get fields from first header
        fields = headers[0].keys()
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(headers)

def main():
    # Initialize extractor
    extractor = SectionHeaderExtractor("1 TOM PLUMBER GLOBAL, INC._2024.json")
    
    # Extract headers
    headers = extractor.extract_section_headers()
    
    # Save to JSON
    extractor.save_to_json(headers, "section_headers.json")
    
    # Save to CSV
    extractor.save_to_csv(headers, "section_headers.csv")
    
    print(f"Extracted {len(headers)} section headers")
    print("Saved to section_headers.json and section_headers.csv")

if __name__ == "__main__":
    main() 