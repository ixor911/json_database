import json


class Database:
    def __init__(self, file_path: str):
        self.filePath = file_path
        self.lol = 'lol'
        self.tables = []
        self.col_types = ['str', 'int', 'list', 'primary_key', 'foreign_key', 'foreign_keys']

        for table_key in self.get_database().keys():
            self.tables.append(Table(table_key, self))

    def get_filePath(self):
        return self.filePath

    def set_filePath(self, file_path: str):
        self.filePath = file_path

    def get_database(self):
        return json.load(open(self.filePath))

    def set_database(self, database: dict):
        with open(self.filePath, 'w') as file:
            json.dump(database, file, indent=4)

    def get_table(self, table_name: str):
        table_list = list(filter(lambda x: x.name == table_name, self.tables))
        if len(table_list) > 0:
            return table_list[0]
        else:
            return None

    def create_table(self, table_name: str):
        database = self.get_database()

        if table_name not in database.keys():
            database[table_name] = {
                "name": table_name,
                "columns": [{
                    "name": "id",
                    "type": "primary_key",
                    "next_value": 1
                }],
                "rows": []
            }
            table = Table(table_name, self)
            self.tables.append(table)
            self.set_database(database)
            return table

        return None

    def change_table_name(self, old_name: str, new_name: str):
        database = self.get_database()

        table_dict = database.pop(old_name)
        table_dict['name'] = new_name
        database[new_name] = table_dict

        self.set_database(database)

    def delete_table(self, table_name: str):
        database = self.get_database()
        self.tables.pop(self.tables.index(self.get_table(table_name)))
        return database.pop(table_name)

    def set_table(self, table_dict):
        database = self.get_database()
        database[table_dict.get('name')] = table_dict
        self.set_database(database)


class Table:
    def __init__(self, name: str, database: Database):
        self.name = name
        self.database = database

    def get_name(self):
        return self.name

    def set_name(self, name: str):
        database = self.database.get_database()
        if name not in database.keys():
            self.database.change_table_name(self.name, name)
            self.name = name

    def get_dict(self):

        return self.database.get_database().get(self.name)

    def get_columns(self):
        return self.get_dict().get('columns')

    def get_column(self, col_name: str):
        return list(filter(lambda x: x.get('name') == col_name, self.get_columns()))[0]

    def get_column_index(self, col_name: str):
        column = self.get_column(col_name)
        columns = self.get_columns()

        return columns.index(column)

    def get_column_values(self, col_name: str):
        col_index = self.get_columns().keys().index(col_name)
        rows = self.get_rows()

        col_values = []
        for row in rows:
            col_values.append(row[col_index])

        return col_values

    def get_columns_types(self):
        col_types = []
        for col in self.get_columns():
            col_types.append(col.get('type'))
        return col_types

    def get_columns_names(self):
        col_names = []
        for col in self.get_columns():
            col_names.append(col.get('name'))
        return col_names

    def add_column(self, name: str, col_type: str, foreign_table: str = None):
        columns = self.get_columns()
        if col_type in self.database.col_types and col_type != 'primary_key' and name not in self.get_columns_names():
            if col_type in ['foreign_key', 'foreign_keys'] and \
                    foreign_table is not None and self.check_table(foreign_table):

                column = {
                    "name": name,
                    "type": col_type,
                    "foreign_table": foreign_table
                }

            elif col_type not in ['foreign_key', 'foreign_keys']:
                column = {
                    "name": name,
                    "type": col_type
                }

            else:
                return

            columns.append(column)

            self.set_columns(columns)
            self.update_rows_by_col(column)

    def del_column(self, col_name: str):
        column = self.get_column(col_name)
        if column.get('type') != 'primary_key':
            columns = self.get_columns()
            columns.pop(self.get_column_index(col_name))

            self.delete_row_values_by_col(self.get_column_index(col_name))
            self.set_columns(columns)

    def edit_column_name(self, col_name: str, new_name: str):
        columns = self.get_columns()
        columns[self.get_column_index(col_name)]['name'] = new_name
        self.set_columns(columns)

    def set_columns(self, columns: list):
        table_dict = self.get_dict()
        table_dict['columns'] = columns
        self.database.set_table(table_dict)

    def get_rows(self):
        return self.get_dict().get('rows')

    def get_row(self, row_id: int):
        return list(filter(lambda x: x[0] == row_id, self.get_rows()))[0]

    def get_row_index(self, row_id: int):
        row = self.get_row(row_id)
        rows = self.get_rows()

        return rows.index(row)

    def get_row_value(self, row_id: int, col_name: str):
        row = self.get_row(row_id)
        return row[self.get_column_index(col_name)]

    def add_row(self, args: list):
        if len(args) == len(self.get_columns()) - 1:
            counter = 0

            row = [self.get_next_id()]
            for arg in args:
                counter += 1
                if self.check_type(arg, self.get_columns_types()[counter]):
                    row.append(arg)
                else:
                    return

            self.set_next_id()
            rows = self.get_rows()
            rows.append(row)
            self.set_rows(rows)

    def del_row(self, row_id: int):
        rows = self.get_rows()
        rows.pop(self.get_row_index(row_id))
        self.set_rows(rows)

    def edit_row(self, row_id: int, args: list):
        row = self.get_row(row_id)
        new_row = [row[0]]

        if len(args) == len(self.get_columns()) - 1:
            counter = 0
            for arg in args:
                counter += 1
                if self.check_type(arg, self.get_columns_types()[counter]):
                    new_row.append(arg)
                else:
                    return

            rows = self.get_rows()
            rows[self.get_row_index(row_id)] = new_row
            self.set_rows(rows)

    def edit_row_value(self, row_id: int, col_name: str, value):
        row = self.get_row(row_id)
        column = self.get_column(col_name)

        if self.check_type(value, column.get('type')):
            row[self.get_column_index(col_name)] = value

        rows = self.get_rows()
        rows[self.get_row_index(row_id)] = row

        self.set_rows(rows)

    def set_rows(self, rows: list):
        table_dict = self.get_dict()
        table_dict['rows'] = rows
        self.database.set_table(table_dict)

    def get_next_id(self):
        return self.get_column('id').get('next_value')

    def set_next_id(self):
        columns = self.get_columns()
        columns[0]['next_value'] += 1
        self.set_columns(columns)

    def check_type(self, value, col_type):
        return type(value) == self.type_convertor(col_type)

    def check_table(self, table_name: str):
        return self.database.get_table(table_name) is not None

    def get_foreign_table_row(self, table_name: str, row_id: int):
        table = self.database.get_table(table_name)

        if table is None:
            return []

        return table.get_row(row_id)

    def get_foreign_table_rows(self, table_name: str, row_ids: list = None):
        table = self.database.get_table(table_name)

        if table is None:
            return []

        if row_ids is None:
            rows = table.get_rows()
        else:
            rows = []
            for row_id in row_ids:
                rows.append(table.get_row(row_id))

        return rows

    def convert_foreign_key(self, row_value: dict):
        table_name = row_value.get('foreign_table')
        value = row_value.get('value')

        if type(value) is int:
            return self.get_foreign_table_row(table_name, value)
        elif type(value) is list:
            return self.get_foreign_table_rows(table_name, value)
        else:
            return None

    def update_rows_by_col(self, column: dict):
        col_type = column.get('type')

        rows = self.get_rows()
        new_rows = []
        for row in rows:
            if col_type == "str":
                row.append("")
            elif col_type == "int":
                row.append(0)
            elif col_type == "list":
                row.append([])
            elif col_type == "foreign_key":
                row.append({
                    "value": None,
                    "foreign_table": column.get('foreign_table')
                })
            elif col_type == "foreign_keys":
                row.append({
                    "value": [],
                    "foreign_table": column.get('foreign_table')
                })

            new_rows.append(row)

        self.set_rows(new_rows)

    def delete_row_values_by_col(self, column_index: int):
        rows = self.get_rows()
        new_rows = []
        for row in rows:
            row.pop(column_index)
            new_rows.append(row)

        self.set_rows(new_rows)

    @staticmethod
    def type_convertor(col_type):
        if col_type == 'str':
            return str
        elif col_type in ['int', 'primary_key']:
            return int
        elif col_type in ['list']:
            return list
        elif col_type in ['foreign_keys', 'foreign_key']:
            return dict
        else:
            return None


