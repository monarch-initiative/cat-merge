"""
Utilities for parsing LinkML schemas to identify multivalued fields.
Supports KGX format and Biolink Model schema definitions.
"""

import yaml
from typing import Dict, Set, Optional, List
from pathlib import Path
from linkml_runtime.utils.schemaview import SchemaView


class SchemaParser:
    """Parser for LinkML schemas to identify multivalued fields using SchemaView."""
    
    def __init__(self, schema_path: Optional[str] = None):
        """
        Initialize the schema parser.
        
        Args:
            schema_path: Path to LinkML YAML schema file. If None, uses latest Biolink Model.
        """
        self.schema_path = schema_path
        self.schema_view = None
        
        if schema_path:
            self.schema_view = SchemaView(schema_path)
        else:
            # Default to latest Biolink Model
            try:
                self.schema_view = SchemaView("https://raw.githubusercontent.com/biolink/biolink-model/master/biolink-model.yaml")
            except Exception:
                # Fallback if network unavailable
                self.schema_view = None
    
    def is_field_multivalued(self, field_name: str) -> bool:
        """
        Check if a field is defined as multivalued in the schema.
        Uses induced_slot to get the base slot definition.
        
        Args:
            field_name: Field/slot name to check
            
        Returns:
            True if field is multivalued, False otherwise
        """
        if not self.schema_view:
            return False
        
        try:
            # Use induced_slot to get the complete slot definition
            slot = self.schema_view.induced_slot(field_name)
            return slot.multivalued if slot else False
        except Exception:
            # If slot doesn't exist or other error, assume not multivalued
            return False
    
    def get_multivalued_fields_from_list(self, field_names: List[str]) -> Set[str]:
        """
        Filter a list of field names to only those that are multivalued.
        
        Args:
            field_names: List of field names to check
            
        Returns:
            Set of field names that are multivalued
        """
        return {field for field in field_names if self.is_field_multivalued(field)}


def get_kgx_multivalued_fields() -> Dict[str, Set[str]]:
    """
    Get commonly known multivalued fields for KGX format when no schema is available.
    Based on Biolink Model and KGX format specifications.
    
    Returns:
        Dictionary mapping class types to known multivalued fields
    """
    return {
        'Node': {
            'category', 'type', 'xref', 'synonym', 'in_taxon', 
            'provided_by', 'publications', 'same_as'
        },
        'Edge': {
            'category', 'qualifiers', 'publications', 'provided_by',
            'knowledge_source', 'aggregator_knowledge_source',
            'primary_knowledge_source', 'supporting_data_source'
        },
        # Common aliases
        'NamedThing': {
            'category', 'type', 'xref', 'synonym', 'in_taxon',
            'provided_by', 'publications', 'same_as'
        },
        'Association': {
            'category', 'qualifiers', 'publications', 'provided_by',
            'knowledge_source', 'aggregator_knowledge_source', 
            'primary_knowledge_source', 'supporting_data_source'
        }
    }


def split_multivalued_field(value: str, delimiter: str = '|') -> List[str]:
    """
    Split a pipe-delimited multivalued field into a list.
    
    Args:
        value: String value to split
        delimiter: Delimiter to split on (default: '|')
        
    Returns:
        List of values, with empty strings and whitespace cleaned
    """
    if not value or value.strip() == '':
        return []
    
    # Split and clean values
    values = [v.strip() for v in value.split(delimiter)]
    # Remove empty values
    return [v for v in values if v]


# Global schema parser instance (lazy loaded)
_global_schema_parser = None


def get_schema_parser(schema_path: Optional[str] = None) -> SchemaParser:
    """
    Get a global schema parser instance (cached for performance).
    
    Args:
        schema_path: Path to schema file (if different from cached instance)
        
    Returns:
        SchemaParser instance
    """
    global _global_schema_parser
    
    if _global_schema_parser is None or (schema_path and schema_path != _global_schema_parser.schema_path):
        _global_schema_parser = SchemaParser(schema_path)
    
    return _global_schema_parser


def is_field_multivalued(field_name: str, schema_path: Optional[str] = None) -> bool:
    """
    Convenience function to check if a field is multivalued.
    
    Args:
        field_name: Field name to check
        schema_path: Optional schema path (uses cached parser if None)
        
    Returns:
        True if field is multivalued, False otherwise
    """
    parser = get_schema_parser(schema_path)
    return parser.is_field_multivalued(field_name)