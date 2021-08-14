import pandas as pd
import sqlite3
import mysql.connector as myql


class DatabaseSave:

    def __init__(self, database, tabla, ruta):
        self.database = database
        self.tabla = tabla
        self.ruta = ruta

    def read(self):
        archive = pd.read_excel(self.ruta)
        register = []
        rows = []
        for key, data in archive.iterrows():
            for row in data:
                rows.append(row)
            register.append(tuple(rows))
            rows = []

        return register

    def mysql(self, host, user, passw):
        db = myql.connect(
                host=host,
                user=user,
                passwd=passw,
                database=self.database
            )

        cursor = db.cursor()
        register = self.read()
        cursor.executemany("INSERT INTO {} values (%s,%s,%s,%s)".format(self.tabla), list(register))
        db.commit()
        print("Registros almacenados correctamente...")
        db.close()

    def sqlite3(self):

        connect = sqlite3.connect("{}.db".format(self.database))
        cursor = connect.cursor()
        registers = self.read()
        cursor.executemany("INSERT INTO {} VALUES(?,?,?,?)".format(self.tabla), set(registers))
        connect.commit()
        print("Registros almacenados correctamente...")
        connect.close()



#SQLITE
# Codigo para arrancar el programa con una base de datos SQLite3
#queryDatabase = DatabaseSave('DATABASE', 'TABLA', 'EXCEL+extension(.xlsx)')
#queryDatabase.sqlite3()
#MYSQL
# Codigo para arrancar el programa con una base de datos Mysql
#queryDatabase = DatabaseSave('DATABASE', 'TABLA', 'EXCEL+extension(.xlsx)')
#queryDatabase.mysql("HOST", "USER", "PASSWORD")









