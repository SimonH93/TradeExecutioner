from fastapi import FastAPI
import uvicorn
import logging

# Konfigurieren des Loggings
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialisierung der FastAPI-App
app = FastAPI(title="Meine Beispiel-App")

# Die Funktion, die beim Start der Anwendung ausgeführt wird
@app.on_event("startup")
async def startup_event():
    """
    Diese Funktion wird einmal ausgeführt, wenn der Server startet.
    """
    logger.info("App-Start-Event: Anwendung wird initialisiert...")
    # Hier können Sie Initialisierungslogik hinzufügen, z.B. Datenbankverbindungen,
    # Laden von Modellen, etc.
    logger.info("App-Start-Event: Initialisierung abgeschlossen.")

# Eine einfache Route zur Überprüfung, ob der Server läuft
@app.get("/")
def read_root():
    """
    Haupt-Route, gibt eine Willkommensnachricht zurück.
    """
    return {"message": "Willkommen bei der FastAPI-Anwendung! Der Server läuft."}

# ==============================================================================
# WICHTIG: Ausführung mit Uvicorn
# ==============================================================================
# Dieser Block stellt sicher, dass die Anwendung nur gestartet wird,
# wenn die Datei direkt ausgeführt wird (z.B. mit 'python main.py').
if __name__ == "__main__":
    # Uvicorn startet den ASGI-Server.
    # Hier wird 'app' aus der aktuellen Datei ('main') geladen.
    # host='0.0.0.0' macht die App von außen erreichbar (wichtig für Container/Server).
    # port=8000 ist der Standard-Entwicklungsport.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    
# HINWEIS: Wenn Sie die App in einer Produktionsumgebung starten (z.B. mit gunicorn),
# verwenden Sie diesen Block nicht, sondern starten die App direkt mit dem
# `uvicorn main:app --host 0.0.0.0 --port 8000` Befehl.