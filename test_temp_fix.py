#!/usr/bin/env python3
"""
Test script to create a properly formatted temp file
"""

def create_fixed_temp_file():
    # Read the original file
    original_path = "github_work/eba-taxonomies/EBA Taxonomy 4.0/sample_files/DUMMYLEI123456789012.CON_FR_MICA010000_MICAITS_2024-12-31_20241211135440207.xbrl"
    
    with open(original_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find the first existing schemaRef and insert before it
    existing_schema_ref = '<link:schemaRef xlink:type="simple" xlink:href="http://www.eba.europa.eu/eu/fr/xbrl/crr/fws/mica/4.0/mod/mica_its.xsd"/>'
    
    if existing_schema_ref in content:
        # Insert the new schemaRef before the existing one
        new_schema_ref = '  <link:schemaRef xlink:type="simple" xlink:href="http://www.eba.europa.eu/eu/fr/xbrl/crr/dict/met/met.xsd"/>\n  '
        content = content.replace(existing_schema_ref, new_schema_ref + existing_schema_ref)
        
        # Write to temp file
        temp_path = "backend/temp/test_fixed.xbrl"
        with open(temp_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Created fixed temp file: {temp_path}")
        
        # Test if it's valid XML
        import xml.etree.ElementTree as ET
        try:
            tree = ET.parse(temp_path)
            print("✓ Valid XML")
            root = tree.getroot()
            schema_refs = root.findall('.//{http://www.xbrl.org/2003/linkbase}schemaRef')
            print(f"Found {len(schema_refs)} schemaRef elements")
            for ref in schema_refs:
                href = ref.get("{http://www.w3.org/1999/xlink}href")
                print(f"  - {href}")
        except Exception as e:
            print(f"✗ Invalid XML: {e}")
    else:
        print("Could not find existing schemaRef")

if __name__ == "__main__":
    create_fixed_temp_file()
