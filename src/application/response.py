from src.sql.connection import ConnectionSQL
from src.mongo.connection import ConnectionMongo
import json
class EventosResponse:
    def consultaSQL(self):
        connection = ConnectionSQL()
        cursor = connection.cursor
        cursor.execute("SELECT TOP 10 e.*, c.Descripcion, c.FechaCreacion, c.UsuarioCreacion, g.Valor " + 
            "FROM tbl_Eventos e "+
            "INNER JOIN (" +
                "SELECT IdReferenciaPadre, Descripcion, FechaCreacion, UsuarioCreacion, " +
                    "ROW_NUMBER() OVER (PARTITION BY IdReferenciaPadre ORDER BY FechaCreacion DESC) AS RowNumber " +
            "FROM tbl_Comentario " +
            ") c ON e.IdEvento = c.IdReferenciaPadre AND c.RowNumber = 1 "+
            "INNER JOIN tbl_Global g ON e.IdEstado = g.IdGlobal  WHERE YEAR(e.FechaRecepcion) = 2023")
        # Obtener los resultados de la consulta
        results = cursor.fetchall()
        print("Cantidad de filas 2023:", len(results))
        print("--------------------------------------")
        # Imprimir los resultados
        lista_filas = []
        cont = 0
        for row in results:
            row_list = list(row)  # Convierte la fila en una lista
            fecha1 = row_list[14]  # Obtiene la fecha en formato datetime
            fecha1_str = fecha1.strftime("%d-%m-%Y %H:%M:%S")  # Convierte la fecha a formato string
            row_list[14] = fecha1_str  # Reemplaza la fecha datetime por la fecha string en la lista
            fecha2 = row_list[16]  # Obtiene la fecha en formato datetime
            fecha2_str = fecha2.strftime("%d-%m-%Y %H:%M:%S")  # Convierte la fecha a formato string
            row_list[16] = fecha2_str  # Reemplaza la fecha datetime por la fecha string en la lista
            fecha19 = row_list[19]  # Obtiene la fecha en formato datetime
            fecha19_str = fecha19.strftime("%d-%m-%Y %H:%M:%S")  # Convierte la fecha a formato string
            row_list[19] = fecha19_str  # Reemplaza la fecha datetime por la fecha string en la lista
            row_list[3] = row_list[3].strip()
            row_list[7] = row_list[7].strip()
            row_list[8] = row_list[8].strip()
            row_list[9] = row_list[9].strip()
            row_list[17] = row_list[17].strip()
            row_list[20] = row_list[20].strip()
            evento = self.parsearEstructuraMongo(row_list)
            datainsert = self.insertarMongoDB(evento)
            if datainsert:
                cont += 1
            print(cont)
        cursor.close()
        return lista_filas

    
    def parsearEstructuraMongo(self, listaEvento):
        print(json.dumps(listaEvento))
        eventoMongo = {}
        eventoMongo["cod_cuenta"] = "SY001"
        eventoMongo["cod_cliente"] = "SI001"
        eventoMongo["cod_evento"] = listaEvento[3]
        eventoMongo["placa"] = listaEvento[4]
        eventoMongo["sigla_cuenta"] = "SYS NET"
        eventoMongo["sigla_cliente"] = "SIGNIA"
        eventoMongo["prioridad"] = listaEvento[-1]
        eventoMongo["origen"] = "SQL"
        eventoMongo["latitud"] = listaEvento[7]
        eventoMongo["fecha"] = listaEvento[6][:9]
        eventoMongo["hora"] = listaEvento[6][10:]
        eventoMongo["longitud"] = listaEvento[8]
        eventoMongo["velocidad"] = listaEvento[9]
        eventoMongo["geocerca"] = listaEvento[11]
        eventoMongo["grupo"] = listaEvento[12]
        eventoMongo["direccion"] = listaEvento[10]
        eventoMongo["fecha_ultima_accion"] = listaEvento[16]
        eventoMongo["descripcion_estado"] = ""
        eventoMongo["estado"] = 0
        eventoMongo["guid"] = ""
        eventoMongo["link_video"] = ""
        eventoMongo["link_imagen"] = ""
        eventoMongo["list_comentarios"] = []
        return eventoMongo

    def insertarMongoDB(self, evento):
        connect = ConnectionMongo()
        db = connect.con
        col = db["notificaciones_test"]
        result = col.insert_one(evento)
        if result.acknowledged:
            return True
        else:
            return False


        #         {
        #   "_id": {
        #     "$oid": "644ddc50cf8548f59b30ed38"
        #   },
        #   "cod_cuenta": "SY001",
        #   "cod_cliente": "SI001",
        #   "cod_evento": "BOTPAN",
        #   "placa": "FTH678",
        #   "sigla_cuenta": "CLAV7",
        #   "sigla_cliente": "QUITBE",
        #   "prioridad": "CRITICO",
        #   "origen": "Sys4Log",
        #   "latitud": "-12.075002",
        #   "fecha": "2023-04-29",
        #   "hora": "19:57:41",
        #   "longitud": "-76.991590",
        #   "velocidad": "9 km/h",
        #   "geocerca": "Mapa_Peru",
        #   "grupo": "Z3",
        #   "direccion": "Avenida Circunvalación, San Luis, Lima, Peru",
        #   "fecha_ultima_accion": "2023-04-29 22:14:46",
        #   "descripcion_estado": "Confirmado",
        #   "estado": 1,
        #   "guid": "",
        #   "link_video": "",
        #   "link_imagen": "",
        #   "list_comentarios": [
        #     {
        #       "comentario": "Esta notificacion no estaba programada",
        #       "fechaenvio": "2023-04-29 22:14:46",
        #       "descripcionestado": "Confirmado",
        #       "rol": "Administrador",
        #       "usuario": "quitumbe"
        #     }
        #   ]
        # }