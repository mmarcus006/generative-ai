from pathlib import Path
import json
import sqlite3
from typing import Dict, List, Tuple
from datetime import datetime
import re
from tqdm import tqdm
import argparse

class FDDDocumentOrganizer:
    def __init__(self, db_path: str = "fdd_documents.db", overwrite: bool = False):
        """Initialize organizer with database path."""
        self.db_path = db_path
        self.overwrite = overwrite
        self.init_database()
        
    def init_database(self):
        """Create database schema if it doesn't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                -- Documents table stores metadata about each FDD document
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    year INTEGER,
                    company_name TEXT,
                    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(file_path)
                );
                
                -- Sections table stores identified FDD sections
                CREATE TABLE IF NOT EXISTS sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_id INTEGER,
                    item_number INTEGER,
                    section_type TEXT NOT NULL, -- 'IDENTIFIED' or 'INFERRED'
                    header_text TEXT,
                    start_page INTEGER,
                    end_page INTEGER,
                    confidence_score FLOAT,
                    FOREIGN KEY (document_id) REFERENCES documents(id)
                );
                
                -- Section content stores the actual text content
                CREATE TABLE IF NOT EXISTS section_content (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    section_id INTEGER,
                    text_content TEXT,
                    page_number INTEGER,
                    original_ref TEXT,
                    sequence_order INTEGER,
                    bbox_top FLOAT,
                    bbox_left FLOAT,
                    FOREIGN KEY (section_id) REFERENCES sections(id)
                );
                
                -- Validation issues table
                CREATE TABLE IF NOT EXISTS validation_issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    section_id INTEGER,
                    issue_type TEXT,
                    issue_description TEXT,
                    severity TEXT,
                    FOREIGN KEY (section_id) REFERENCES sections(id)
                );
            """)
    
    def _get_sorted_texts(self, doc_data: Dict, start_page: int, end_page: int = None) -> List[Dict]:
        """Get sorted texts for a page range using standardized method."""
        page_texts = []
        
        for text in doc_data.get('texts', []):
            if 'prov' in text and text['prov']:
                prov = text['prov'][0]
                page_no = prov.get('page_no', 0)
                
                if start_page <= page_no and (end_page is None or page_no < end_page):
                    bbox = prov.get('bbox', {})
                    page_texts.append({
                        'text': text.get('text', ''),
                        'page_no': page_no,
                        'top': bbox.get('t', 0),
                        'left': bbox.get('l', 0),
                        'self_ref': text.get('self_ref', '')
                    })
        
        # Sort by page number and position (top to bottom, left to right)
        page_texts.sort(key=lambda x: (x['page_no'], -x['top'], x['left']))
        return page_texts
    
    def process_document(self, json_path: str) -> int:
        """Process a single document and return document_id."""
        with open(json_path, 'r', encoding='utf-8') as f:
            doc_data = json.load(f)
            
        # Extract document metadata
        file_path = Path(json_path)
        year_match = re.search(r'_(\d{4})', file_path.stem)
        year = int(year_match.group(1)) if year_match else None
        company_name = file_path.stem.split('_')[0]
        
        # First check if document exists
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Try to get existing document ID
            cursor.execute("""
                SELECT id FROM documents 
                WHERE file_path = ?
            """, (str(file_path),))
            
            existing_doc = cursor.fetchone()
            
            if existing_doc:
                document_id = existing_doc[0]
            else:
                # Insert new document if it doesn't exist
                cursor.execute("""
                    INSERT INTO documents 
                    (file_name, file_path, year, company_name)
                    VALUES (?, ?, ?, ?)
                """, (file_path.name, str(file_path), year, company_name))
                document_id = cursor.lastrowid
                
            conn.commit()
        
        # Process sections
        self._process_sections(doc_data, document_id)
        
        return document_id
    
    def _identify_section_headers(self, doc_data: Dict) -> List[Dict]:
        """Identify section headers in document using standardized method."""
        headers = []
        for text in doc_data.get('texts', []):
            if text.get('label') == 'section_header':
                prov = text.get('prov', [{}])[0]
                bbox = prov.get('bbox', {})
                headers.append({
                    'text': text.get('text', ''),
                    'page_no': prov.get('page_no', 0),
                    'top': bbox.get('t', 0),
                    'left': bbox.get('l', 0),
                    'self_ref': text.get('self_ref', '')
                })
        
        # Sort headers by page and position
        headers.sort(key=lambda x: (x['page_no'], -x['top'], x['left']))
        return headers
    
    def _process_sections(self, doc_data: Dict, document_id: int):
        """Process and store document sections."""
        section_headers = self._identify_section_headers(doc_data)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check if sections exist for this document
            cursor.execute("SELECT COUNT(*) FROM sections WHERE document_id = ?", (document_id,))
            existing_sections = cursor.fetchone()[0] > 0
            
            if existing_sections and not self.overwrite:
                # Skip processing if sections exist and overwrite is False
                return
            elif existing_sections and self.overwrite:
                # Delete existing sections if overwrite is True
                cursor.execute("DELETE FROM sections WHERE document_id = ?", (document_id,))
                conn.commit()
            
            for i, header in enumerate(section_headers):
                next_header = section_headers[i + 1] if i + 1 < len(section_headers) else None
                
                # Extract item number if present
                item_match = re.search(r'ITEM\s*(\d+)', header['text'], re.IGNORECASE)
                item_number = int(item_match.group(1)) if item_match else None
                
                # Insert section record
                cursor.execute("""
                    INSERT INTO sections (
                        document_id, item_number, section_type, header_text,
                        start_page, end_page, confidence_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    document_id,
                    item_number,
                    'IDENTIFIED',
                    header['text'],
                    header['page_no'],
                    next_header['page_no'] - 1 if next_header else None,
                    1.0
                ))
                section_id = cursor.lastrowid
                
                # Get and store section content
                section_texts = self._get_sorted_texts(
                    doc_data,
                    header['page_no'],
                    next_header['page_no'] if next_header else None
                )
                
                # Store content with position information
                for sequence, text_item in enumerate(section_texts):
                    cursor.execute("""
                        INSERT INTO section_content (
                            section_id, text_content, page_number,
                            original_ref, sequence_order, bbox_top, bbox_left
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        section_id,
                        text_item['text'],
                        text_item['page_no'],
                        text_item['self_ref'],
                        sequence,
                        text_item['top'],
                        text_item['left']
                    ))

def process_directory(input_dir: str, overwrite: bool = False):
    """Process all JSON files in directory."""
    organizer = FDDDocumentOrganizer(overwrite=overwrite)
    input_path = Path(input_dir)
    
    # Process all JSON files
    json_files = list(input_path.glob('*.json'))
    for json_file in tqdm(json_files, desc="Processing FDD documents"):
        try:
            organizer.process_document(str(json_file))
        except Exception as e:
            print(f"\nError processing {json_file.name}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Process FDD documents')
    parser.add_argument('--input-dir', default=r"C:\Projects\NewFranchiseData_Windows\FinalFranchiseData_Windows\data\processed_data\json_documents")
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing sections')
    args = parser.parse_args()
    
    process_directory(args.input_dir, args.overwrite)
    print("\nProcessing complete! Data stored in fdd_documents.db")

if __name__ == "__main__":
    main() 