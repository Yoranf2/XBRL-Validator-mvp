#!/usr/bin/env python3
"""
Debug script to test namespace detection
"""
import xml.etree.ElementTree as ET

def test_namespace_detection(file_path):
    print(f"Testing namespace detection for: {file_path}")
    
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        print(f"Root tag: {root.tag}")
        print(f"Root attributes: {list(root.attrib.keys())}")
        
        # Get all namespace declarations from root attributes
        namespaces = {}
        for attr_name, attr_value in root.attrib.items():
            if attr_name.startswith('xmlns:'):
                prefix = attr_name[6:]  # Remove 'xmlns:' prefix
                namespaces[prefix] = attr_value
            elif attr_name == 'xmlns':
                namespaces[None] = attr_value
        
        print(f"Found {len(namespaces)} namespaces:")
        for prefix, uri in namespaces.items():
            print(f"  {prefix}: {uri}")
        
        # Check for eba_met specifically
        eba_met_ns = "http://www.eba.europa.eu/xbrl/crr/dict/met"
        if 'eba_met' in namespaces and namespaces['eba_met'] == eba_met_ns:
            print("✓ Found eba_met namespace")
            
            # Check for eba_met elements
            eba_met_elements = []
            for elem in root.iter():
                if elem.tag.startswith('eba_met:'):
                    eba_met_elements.append(elem.tag)
            
            print(f"Found {len(eba_met_elements)} eba_met elements")
            if eba_met_elements:
                print(f"Sample elements: {eba_met_elements[:5]}")
        else:
            print("✗ eba_met namespace not found")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Test with uploaded file
    import glob
    upload_files = glob.glob("backend/uploads/DUMMYLEI123456789012.CON_FR_MICA010000_MICAITS_2024-12-31_20241211135440207_*.xbrl")
    if upload_files:
        test_namespace_detection(upload_files[-1])
    else:
        print("No uploaded MICA files found")
