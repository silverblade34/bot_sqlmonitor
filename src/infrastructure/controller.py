from src.application.response import EventosResponse

class BotEventos:
    def consumirSQLEventos(self):
        response = EventosResponse()
        data = response.procesoEnvioEventos() 
        return data