
import sys
import os
import json
from lxml import html as lxml_html

# Load template
def load_template():
    with open('craw/template/mogidetails.json', 'r') as f:
        return json.load(f)['fields']

def extract_value_by_selector(tree, selector, value_type='text'):
    # Simplified version of the crawler logic
    from lxml.cssselect import CSSSelector
    try:
        if selector.startswith('//'):
            elements = tree.xpath(selector)
        else:
            css_sel = CSSSelector(selector)
            elements = css_sel(tree)
        
        if not elements: return None
        element = elements[0]
        
        if value_type == 'text':
            return element.text_content().strip()
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def test():
    print("Testing Project Name Extraction...")
    with open('dummy_project.html', 'r') as f:
        html = f.read()
    
    tree = lxml_html.fromstring(html)
    fields = load_template()
    
    # Find thuocduan field
    field_config = next((f for f in fields if f['name'] == 'thuocduan'), None)
    if not field_config:
        print("❌ Field 'thuocduan' not found in template!")
        return
        
    print(f"Selector: {field_config['selector']}")
    value = extract_value_by_selector(tree, field_config['selector'], field_config.get('valueType', 'text'))
    
    if value == "Cosmo city":
        print(f"✅ SUCCESS: Extracted '{value}'")
    else:
        print(f"❌ FAILED: Extracted '{value}' (Expected 'Cosmo city')")

if __name__ == "__main__":
    test()
