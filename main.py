from database import Database


database = Database('DB.json')
table1 = database.get_table('table1')
table2 = database.get_table('table2')



