import pyodbc
 
class ConnectionSQL:
    def __init__(self):
        # Establecer la conexión a la base de datos
        server = 'IR SERVER'
        database = 'DATEBASE'
        username = 'USERNAME'
        password = 'PASSWORD'
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        # Crear un cursor para ejecutar comandos SQL
        self.cursor = cnxn.cursor()
