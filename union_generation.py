from modules.project_enums import SQLText


def generate_union_statement(columns, source):
    """

    This function may not be needed depending on the results found in the melt_test() function

    This function is intended to handle the dynamic and tedious task of maintaining the union all
    sql statements used to import data into the word press engine's wp_postmeta table.
    :param columns: an iterable data type (array/tuple) containing all of the column names.
    :param source: the explicit 'database.table' source text used in the queries WHERE clause.
    :return: text type of the entire union statement query.
    """
    sql_raw_text = SQLText.union_part.value
    if len(columns) >= 2:
        union_parts = []
        for column in columns:
            if len(column) >= 3 and type(column) == 'str':
                union_part_text = sql_raw_text.text % (column, column, source)
                union_parts.append(union_part_text)
            full_union_text = 'union all'.join(union_parts)
            return full_union_text
    else:
        full_union_text = sql_raw_text.text % (columns[0], columns[0], source)
        return full_union_text
