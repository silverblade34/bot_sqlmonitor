import pyodbc
 
class ConnectionSQL:
    def __init__(self):
        # Establecer la conexión a la base de datos
        server = '34.225.53.169'
        database = 'DEV_Eventos'
        username = 'sa'
        password = '0313334'
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

        # Crear un cursor para ejecutar comandos SQL
        self.cursor = cnxn.cursor()