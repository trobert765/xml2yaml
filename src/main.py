import xml.etree.ElementTree as elementTree

import yaml

RULES = 'rules'
RENAME = 'rename'
SET = 'set'
TAB_RE = '\\\\t+?'
SPACE = ' '
RULES_SCHEMA = {'inputs': {'input': {}}, 'outputs': {'output': {'handler': 'helper.dtu_custom_output_dataframe_handler.CustomOutputHandler', 'format': 'csv',
            'sep': '\t', 'emptyValue': '', 'overwrite': True, 'quoteAll': False, 'num_partitions': 1,
            'lineSep': '\u000d\u000A', 'schema': []}}, 'rules': []}


NORMALIZED_FIELDS = ['File_Name', 'Source_Type', 'Network_Name', 'PolicyHolder_ID', 'PolicyHolder_ALT_ID',
                     'PolicyHolder_Last_Name', 'PolicyHolder_First_Name', 'PolicyHolder_Middle_Name',
                     'PolicyHolder_Birth_Date',
                     'PolicyHolder_Date_of_Death', 'PolicyHolder_SSN', 'PolicyHolder_Medicare_HIC', 'PolicyHolder_Sex',
                     'PolicyHolder_Address_1', 'PolicyHolder_Address_2', 'PolicyHolder_City', 'PolicyHolder_State',
                     'PolicyHolder_Zip', 'PolicyHolder_Phone_Number', 'PolicyHolder_Employer_Name',
                     'PolicyHolder_Employer_Address_1', 'PolicyHolder_Employer_Address_2', 'PolicyHolder_Employer_City',
                     'PolicyHolder_Employer_State', 'PolicyHolder_Employer_Zip', 'PolicyHolder_Employer_Contact_Name',
                     'PolicyHolder_Employer_Contact_Phone', 'Member_ID', 'Member_ALT_ID', 'Member_Last_Name',
                     'Member_First_Name',
                     'Member_Middle_Name', 'Member_Birth_Date', 'Member_Date_of_Death', 'Member_SSN',
                     'Member_Medicare_HIC',
                     'Member_Sex', 'Member_Relationship_to_Subscriber', 'Member_Address_1', 'Member_Address_2',
                     'Member_City',
                     'Member_County', 'Member_State', 'Member_Zip', 'Member_Payer_Name', 'Member_Payer_Product',
                     'Medical_Policy_ID_Number', 'Group_Number', 'Subgroup_Number', 'Group_Name', 'Group_Description',
                     'Effective_Date', 'Termination_Date', 'Coverage_Code_Basis', 'Medical_Coverage_Indicator',
                     'Pharmacy_Coverage_Indicator', 'Dental_Coverage_Indicator', 'Vision_Coverage_Indicator',
                     'PBM_Name',
                     'PBM_Cardholder_ID', 'PBM_Person_Code', 'PBM_Help_Desk_Phone_Number', 'PBM_BIN', 'PBM_PCN',
                     'Vision_Name',
                     'Vision_Policy_ID_Number', 'Dental_Name', 'Dental_Policy_ID_Number', 'Plan_Type', 'PlanTypeBasis',
                     'Submitter_Field', 'LOB', 'DivisionCode', 'AUX1', 'AUX2', 'AUX3']

RULES_YAML = '../rules_yaml/'


def fill_schema():
    outbound_items = []

    for i, field in enumerate(NORMALIZED_FIELDS):
        outbound_items.append({'field': field, 'type': 'string', 'nullable': False})

    return outbound_items


def build_rules(file_format: str, header: bool, rules: list, schema: list, delimiter: str = None):
    rules_schema = RULES_SCHEMA
    if delimiter:
        rules_schema['inputs']['input']['delimiter'] = delimiter
    rules_schema['inputs']['input']['format'] = file_format
    rules_schema['inputs']['input']['header'] = header

    rules_schema['outputs']['output']['schema'] = schema

    rules_schema['rules'] = rules

    return rules_schema


def write_rules(filename: str, rules: dict):
    with open(RULES_YAML + filename, 'w') as file:
        yaml.dump(rules, file, sort_keys=False)


def get_xml(path):
    with open(path, 'r') as xml_file:
        xml_data = xml_file.read()

        return xml_data


def get_format(path):
    root = elementTree.parse(path).getroot()
    input_type = {'format': 'text', 'delimiter': None}
    for child in root:
        if child.tag.endswith('RECORD'):
            if child[0].attrib.get('TERMINATOR'):
                input_type['format'] = 'csv'
                input_type['delimiter'] = child[0].attrib.get('TERMINATOR')

    return input_type


def build_rules_section(path, input_format):
    root = elementTree.parse(path).getroot()
    record = []
    row = []
    rules = {}
    start_pos = 1
    delimited = False
    if input_format == 'csv':
        delimited = True

    for child in root:
        if child.tag.endswith('RECORD'):
            if child[0].attrib.get('TERMINATOR'):
                delimited = True
            for item in child:
                if item.attrib.get('LENGTH') and not delimited:
                    record.append(item.attrib.get('LENGTH'))
        if child.tag.endswith('ROW'):
            for item in child:
                # print(item.tag, item.attrib.get('NAME'))
                row.append(item.attrib.get('NAME'))

    if delimited:
        rules.update({RENAME: {}})
    else:
        rules.update({SET: {}})

    for i, item in enumerate(row):
        if delimited:
            k = f'_c{i}'
            v = f'{row[i]}'
            rules[RENAME][k] = v
        else:
            if i < len(record):
                rule = f'upper(regexp_replace(trim(substring(value,{start_pos},{record[i]})), \'{TAB_RE}\', \'{SPACE}\'))'
                start_pos += int(record[i])
            else:
                rule = f'upper(regexp_replace(trim(substring(value,{start_pos},?)), \'{TAB_RE}\', \'{SPACE}\'))'
            rules[SET][item] = rule

    return [rules]


if __name__ == '__main__':
    # file options:
    # 1) format: text/csv - derived from the fmt file
    # 2) header: True/False - set manually
    # 3) delimiter: optional - derived from the fmt file
    # input fmt file
    xml_path = '../fmt_defs/landing_Ameritas-x.fmt'
    has_header = False
    import argparse

    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding arguments
    parser.add_argument("-f", "--File", help="Path to the .fmt file ")
    parser.add_argument("-g", "--Header", help='boolean value indicating that the raw input file contains a header')

    # Read arguments from command line
    args = parser.parse_args()

    if args.File:
        xml_path = args.File
        print("Format definition path: % s" % args.File)

    if args.Header:
        has_header = eval(args.Header)
        print("File contains a header record: % s" % eval(args.Header))

    # build the normalized output schema
    normalized_schema = fill_schema()

    # determine whether this is a csv or fixed record file
    result = get_format(xml_path)
    # parse out the file name that will be used for the rules.yaml
    file_name = xml_path.lstrip('../fmt_defs/landing_').split('-x.fmt')[0]
    # build the rules
    custom_rules = build_rules_section(xml_path, result['format'])
    # combine inputs, outputs and rules
    rules_dict = build_rules(file_format=result['format'],
                             header=has_header,
                             rules=custom_rules,
                             schema=normalized_schema,
                             delimiter=result['delimiter'])
    # write out the rules.yaml
    write_rules(filename=file_name + '.yaml', rules=rules_dict)
