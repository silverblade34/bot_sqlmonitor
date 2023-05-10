from src.sql.connection import ConnectionSQL
from src.mongo.connection import ConnectionMongo
import json
class EventosResponse:
    def procesoEnvioEventos(self, batch_size=1000):
        connection = ConnectionSQL()
        cursor = connection.cursor
        offset = 0
        totdatainsert = 0
        while True:
            cursor.execute("SELECT e.*, c.Descripcion, c.FechaCreacion, c.UsuarioCreacion, g.Valor " + 
                "FROM tbl_Eventos e "+
                "INNER JOIN (" +
                    "SELECT IdReferenciaPadre, Descripcion, FechaCreacion, UsuarioCreacion, " +
                        "ROW_NUMBER() OVER (PARTITION BY IdReferenciaPadre ORDER BY FechaCreacion DESC) AS RowNumber " +
                "FROM tbl_Comentario " +
                ") c ON e.IdEvento = c.IdReferenciaPadre AND c.RowNumber = 1 "+
                "INNER JOIN tbl_Global g ON e.IdEstado = g.IdGlobal  WHERE YEAR(e.FechaRecepcion) = 2023 "+
                f"ORDER BY e.IdEvento OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY;")
            # Obtener los resultados de la consulta
            results = cursor.fetchall()
            if not results:
                break
            print("Cantidad de filas obtenidas:", len(results))
            # Imprimir los resultados
            lista_eventos = self.formatearDatosSQL(results)
            datainsert = self.insertarMongoDB_lotes(lista_eventos)
            print("Insertados: "+ str(datainsert))
            totdatainsert += datainsert
            offset += batch_size
        cursor.close()
        return totdatainsert

    def formatearDatosSQL(self, results):
        lista_eventos = []
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
            lista_eventos.append(evento)
        return lista_eventos

    
    def parsearEstructuraMongo(self, listaEvento):
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
        eventoMongo["estado"] = 1
        eventoMongo["guid"] = ""
        eventoMongo["link_video"] = ""
        eventoMongo["link_imagen"] = ""
        eventoMongo["list_comentarios"] = [
            {
                "comentario": listaEvento[-4],
                "fechaenvio": listaEvento[-3],
                "descripcionestado": listaEvento[-1],
                "rol": "Operador",
                "usuario": listaEvento[-2]
            }
        ]
        return eventoMongo

    def insertarMongoDB_lotes(self, eventos):
        num_eventos = len(eventos)
        connect = ConnectionMongo()
        db = connect.con
        col = db["notificaciones_test"]
        num_insertados_total = 0
        
        # Dividir la lista de eventos en dos sublistas de tamaño igual
        mitad = num_eventos // 2
        eventos1 = eventos[:mitad]
        eventos2 = eventos[mitad:]
        
        # Insertar la primera mitad de eventos
        result1 = col.insert_many(eventos1, False)
        num_insertados1 = len(result1.inserted_ids)
        num_insertados_total += num_insertados1
        
        # Insertar la segunda mitad de eventos
        result2 = col.insert_many(eventos2, False)
        num_insertados2 = len(result2.inserted_ids)
        num_insertados_total += num_insertados2
        
        return num_insertados_total

