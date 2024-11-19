from pathlib import Path
import json
from typing import Dict, List
from rich.console import Console
from rich.tree import Tree

class DoclingGroupParser:
    def __init__(self, json_path: str):
        """Initialize parser with path to Docling JSON file."""
        self.json_path = Path(json_path)
        self.console = Console()
        self.document = self._load_document()
        
    def _load_document(self) -> Dict:
        """Load and parse the Docling JSON document."""
        with open(self.json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _get_text_by_ref(self, ref: str) -> str:
        """Get text content by matching $ref to self_ref in texts array."""
        # Remove the $ref field if present
        text_ref = ref.get('$ref') if isinstance(ref, dict) else ref
        
        for text in self.document['texts']:
            if text.get('self_ref') == text_ref:
                return text.get('text', '')
        return ''
        
    def _get_group_texts(self, group: Dict) -> List[str]:
        """Extract all texts from a group based on text references."""
        texts = []
        
        # Get text references from the group's children
        children = group.get('children', [])
        
        # Collect texts for each reference
        for child in children:
            text = self._get_text_by_ref(child)
            if text:
                texts.append(text)
                    
        return texts

    def visualize_groups(self):
        """Create a tree visualization of document groups and their text content."""
        if 'groups' not in self.document:
            self.console.print("[yellow]No groups found in document")
            return
            
        tree = Tree("📄 Document Groups")
        
        for idx, group in enumerate(self.document['groups']):
            # Create group node
            group_name = group.get('label', f'Group {idx}')
            group_tree = tree.add(f"[blue]{group_name}")
            
            # Add texts under group
            texts = self._get_group_texts(group)
            if texts:
                combined_text = " ".join(texts)
                # Truncate long texts for display
                display_text = combined_text[:1000] + '...' if len(combined_text) > 1000 else combined_text
                group_tree.add(f"[green]'{display_text}'")
            else:
                group_tree.add("[yellow]No texts found")
                
        self.console.print(tree)

def main():
    parser = DoclingGroupParser("1 TOM PLUMBER GLOBAL, INC._2024.json")
    parser.visualize_groups()

if __name__ == "__main__":
    main() 