import os
import json
from pathlib import Path
from typing import Dict, List
import google.generativeai as genai
from tqdm import tqdm
from config import GEMINI_API_KEY  # Import API key from config

class FDDInfoExtractor:
    def __init__(self, json_path: str):
        """Initialize extractor with path to Docling JSON file."""
        self.json_path = Path(json_path)
        genai.configure(api_key=GEMINI_API_KEY)
        
    def _load_document(self) -> Dict:
        """Load and parse the Docling JSON document."""
        with open(self.json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def extract_first_20_pages(self) -> str:
        """Extract and combine text from first 20 pages using standardized method."""
        document = self._load_document()
        texts = []
        
        # First collect all texts with their page numbers and positions
        page_texts = []
        for text in document.get('texts', []):
            if 'prov' in text and text['prov']:
                prov = text['prov'][0]
                page_no = prov.get('page_no', 0)
                if page_no <= 20:  # Only first 20 pages
                    bbox = prov.get('bbox', {})
                    page_texts.append({
                        'text': text.get('text', ''),
                        'page_no': page_no,
                        'top': bbox.get('t', 0),
                        'left': bbox.get('l', 0)
                    })
        
        # Sort by page number and position (top to bottom, left to right)
        page_texts.sort(key=lambda x: (x['page_no'], -x['top'], x['left']))
        
        # Combine texts in order
        texts = [item['text'] for item in page_texts]
        
        return " ".join(texts)
    
    def extract_fdd_info(self) -> Dict:
        """Extract structured FDD information using Gemini."""
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "franchise_name": {"type": "STRING"},
                "legal_entity_type": {"type": "STRING"},
                "parent_company": {"type": "STRING"},
                "industry": {
                    "type": "OBJECT",
                    "properties": {
                        "primary": {"type": "STRING"},
                        "secondary": {"type": "ARRAY", "items": {"type": "STRING"}},
                        "keywords": {"type": "ARRAY", "items": {"type": "STRING"}}
                    }
                },
                "address": {
                    "type": "OBJECT",
                    "properties": {
                        "street": {"type": "STRING"},
                        "city": {"type": "STRING"},
                        "state": {"type": "STRING"},
                        "zip": {"type": "STRING"}
                    }
                },
                "contact_info": {
                    "type": "OBJECT",
                    "properties": {
                        "phone": {"type": "STRING"},
                        "email": {"type": "STRING"},
                        "website": {"type": "STRING"}
                    }
                },
                "issuance_date": {"type": "STRING"},
                "fdd_year": {"type": "INTEGER"},
                "business_description": {"type": "STRING"},
                "initial_franchise_fee": {"type": "STRING"},
                "total_investment_range": {
                    "type": "OBJECT",
                    "properties": {
                        "min": {"type": "STRING"},
                        "max": {"type": "STRING"}
                    }
                },
                "principal_business_address": {"type": "STRING"},
                "state_of_incorporation": {"type": "STRING"},
                "incorporation_date": {"type": "STRING"},
                "franchise_system_description": {"type": "STRING"}
            },
            "required": [
                "franchise_name",
                "address",
                "issuance_date",
                "fdd_year",
                "business_description",
                "industry"
            ]
        }

        # Get full text content without length limit
        text_content = self.extract_first_20_pages()
        
        # Generate prompt
        prompt = f"""
        Extract key franchise information from the following FDD text. 
        Pay special attention to:
        1. The franchise name and legal entity type
        2. Complete address and contact information
        3. Issuance date and FDD year
        4. Business description and franchise system details
        5. Initial franchise fee and total investment range
        6. State of incorporation and incorporation date
        7. Industry classification and keywords
        8. Parent company name if any
        
        For the industry information:
        - Identify the primary industry sector
        - List any secondary/related industries
        - Extract relevant business keywords
        
        For parent company:
        - Identify and provide only the name of any parent or holding company
        - If no parent company is mentioned, return null
        
        Return all information found in the structured format specified.
        If certain information is not found, return null for those fields.
        
        Text content:
        {text_content}
        """
        
        # Get response from Gemini
        model = genai.GenerativeModel("gemini-1.5-pro")
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                response_mime_type="application/json",
                response_schema=response_schema
            )
        )
        
        # Parse and return response
        try:
            return json.loads(response.text)
        except json.JSONDecodeError:
            return {"error": "Failed to parse Gemini response"}

def process_directory(input_dir: str):
    """Process all JSON files in directory."""
    input_path = Path(input_dir)
    output_dir = Path('fdd_info_results')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Process all JSON files
    json_files = list(input_path.glob('*.json'))
    results = []
    
    for json_file in tqdm(json_files, desc="Extracting FDD Information"):
        try:
            extractor = FDDInfoExtractor(str(json_file))
            info = extractor.extract_fdd_info()
            info['file_name'] = json_file.name
            info['file_path'] = str(json_file)
            results.append(info)
        except Exception as e:
            print(f"\nError processing {json_file.name}: {str(e)}")
    
    # Save combined results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"fdd_info_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to {output_file}")

def main():
    if len(sys.argv) > 1:
        # Single file processing
        extractor = FDDInfoExtractor(sys.argv[1])
        fdd_info = extractor.extract_fdd_info()
        print("\nExtracted FDD Information:")
        print("=" * 50)
        print(json.dumps(fdd_info, indent=2))
    else:
        # Directory processing
        input_dir = r"C:\Projects\NewFranchiseData_Windows\FinalFranchiseData_Windows\data\processed_data\json_documents"
        process_directory(input_dir)

if __name__ == "__main__":
    import sys
    from datetime import datetime
    main() 