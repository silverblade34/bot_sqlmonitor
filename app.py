from src.infrastructure.controller import BotEventos
import time 
def main():
    _events = BotEventos()
    resp = _events.editarFechaUltimaAccion()
    print("TOTAL INSERTADOS: "+str(resp))
    print(resp)
    time.sleep(30)
    
main()