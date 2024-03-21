CASE_INDENT = '      '
TARGET_INDENT = '    '
WHEN_INDENT = '        '
THEN_INDENT = '          '
END_INDENT = '      '
LF = '\n'
RULES_DIR = '../crosswalk_rules/'


def read_file(path: str):
    with open(path, 'r') as crosswalk_file:
        cw_rows = crosswalk_file.readlines()

    return cw_rows


def write_rules(filename: str, rules: str):
    with open(RULES_DIR + filename, 'w') as file:
        file.write(rules)


def parse_xwalk(cw_data: list, cw_hdr: bool, key_col: str, value_col: str):
    rule = f'- set:{LF}'
    src = key_col
    target = value_col
    rule += f'{TARGET_INDENT}{target}: |{LF}{CASE_INDENT}CASE{LF}'
    for row in cw_data[cw_hdr:]:
        k, v = row.split('\t')
        v = v.replace(LF, '')
        if len(v) > 1:
            rule += f'{WHEN_INDENT}WHEN {src} = \'{k}\'\n{THEN_INDENT}THEN \'{v}\'{LF}'

    rule += f'{END_INDENT}END as {target}'
    # print(rule)
    return rule


if __name__ == '__main__':
    cw_path = '../crosswalks/900_xwalk_Aetna_Trad_plan_type.txt'  # path to the crosswalk file.
    has_header = False  # look at the crosswalk file - some have a header record, others don't.
    compare_column = 'Group/Employer_Name'  # the column being evaluated.
    set_column = 'plan_type'  # the variable that the cross walked result will be stored in.

    file_name = cw_path.split('900_xwalk_')[1].strip('.txt') + '.rule'
    cw = read_file(path=cw_path)
    cw_rule = parse_xwalk(cw_data=cw, cw_hdr=has_header, key_col=compare_column, value_col=set_column)
    write_rules(filename=file_name, rules=cw_rule)
