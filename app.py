from src.infrastructure.controller import BotEventos

def main():
    _events = BotEventos()
    resp = _events.consumirSQLEventos()
    return resp

main()