const mqtt = require('mqtt');   //Für MQTT
const mysql = require('mysql2');//Für MySQL-DB
const Ajv = require('ajv'); //JSON Schema Validierung
const ajv = new Ajv();
const config = require('./config');

// MQTT Client konfigurieren
//const client = mqtt.connect('mqtt://192.10.10.10');
const client = mqtt.connect('mqtt://172.20.10.7:8883'); //--> mit Angabe des Portes


// Verbindung zur externen MySQL-Datenbank herstellen mit config datei
const db = mysql.createConnection(config.db);

// Verbindung zur Datenbank herstellen
db.connect((err) => {
    if (err) {
        console.error('Error connecting to the database:', err.stack);
        return;
    }
    console.log('Connected to the MySQL database.');
});

// Verbindung zum MQTT-Broker herstellen
client.on('connect', () => {
    console.log('Connecting to MQTT broker...');
    client.subscribe('sensor/pm', (err) => { //Topic angeben
        if (err) {
            console.error('Subscription error:', err);
        } else {
            console.log('Subscribed to topic.');
        }
    });
});


// Nachricht empfangen und in die Warteschlange einfügen
let messageQueue = [];
const MAX_QUEUE_SIZE = 100; //maximale Paketgröße = 256 MB --> maximale Queue-Size = 25,6GB

client.on('message', (topic, message) => {
    console.log(`Received message: ${message.toString()} on topic: ${topic}`);
    if (messageQueue.length < MAX_QUEUE_SIZE) {
        messageQueue.push(message); // Nachricht in die Warteschlange einfügen
    } else {
        console.error('Queue ist voll, Nachricht wird verworfen.');
    }
});
// Intervall von 0,5 Sekunde zum Verarbeiten von Nachrichten
setInterval(() => {
    if (messageQueue.length > 0) {
        const message = messageQueue.shift(); // Nächste Nachricht aus der Warteschlange entnehmen
        uploadMessage(message);
    }
}, 500); // pro Sekunde werden 2 Nachrichten verarbeitet


//DB Verbindung schließen:
process.on('SIGINT', () => {
    db.end((err) => {
        if (err) {
            console.error('Error closing the database connection:', err.message);
        }
        console.log('Database connection closed.');
        process.exit(0);
    });
});


//In die Datenbank speichern
async function uploadMessage(message) {
    try {
        let parsedMessage = JSON.parse(message.toString());
        //JSON-Kontrolle:
        if (!validateMessage(parsedMessage)) {
            return;
        }
        // Extrahiere die Werte aus dem JSON
        let { Id, PM1_0, PM2_5, PM10 } = parsedMessage;
        // Überprüfen ob Client existiert:
        let clientExists = await checkClient(Id);
        if (clientExists) {
            console.log("Client vorhanden!");
            // SQL zum Einfügen der Daten in die Datenbank
            let sql = `INSERT INTO feinstaubwert (\`PM1.0\`, \`PM2.5\`, PM10, ClientID) VALUES (?, ?, ?, ?)`;
            db.query(sql, [PM1_0, PM2_5, PM10, Id], (err, results) => {
                if (err) {
                    console.error('Fehler beim Einfügen der Daten:', err.message);
                    return;
                }
                console.log(`Daten eingefügt!`);
            });
        }
        else{
            console.log(`Client nicht vorhanden - Daten nicht eingefügt`);
        } 
    } catch (err) {
        console.error('Fehler beim Verarbeiten der Nachricht:', err.message);
    }
}



//Schauen ob Client existiert:
async function checkClient(Id) {
    let sql = `SELECT EXISTS(SELECT 1 FROM client WHERE ClientID = ?) AS existsResult`;

    try {
        const [result] = await db.promise().query(sql, [Id]);
        let exists = result[0].existsResult;
        return exists === 1; // Gibt true zurück, wenn der Client existiert, sonst false
    } catch (err) {
        console.error("Fehler bei der Client-Abfrage:", err.message);
        throw err;
    }
}



// JSON-Schema für die erwarteten Felder und JSON daran kontrollieren
const schema = {
    type: "object",
    properties: {
        Id: { type: "number" }, 
        PM1_0: { type: "number" },
        PM2_5: { type: "number" },
        PM10: { type: "number" }
    },
    required: ["Id", "PM1_0", "PM2_5", "PM10"], //alle Felder mussen da sein
    additionalProperties: false // Blockiert unerwartete Felder --> keine anderen Properties
};
function validateMessage(data) {
    //'ajv.compile(schema)' kompiliert das Schema, das das JSON-Dokuments definiert, zu einer ausführbaren Validierungsfunktion.
    const validate = ajv.compile(schema);
    const valid = validate(data); //ob die Daten dem Schema entsprechen
    if (!valid) {
        console.error('JSON-Validierungsfehler:', validate.errors);
        return false;
    }
    console.log("JSON gültig!")
    return true;
}



//-----------------------------------------------------------------------------------
//XXXXXXXXXX
//Zur Simulation:
/*
const messageTEST = '{"Id":1,"PM1_0":0,"PM2_5":0,"PM10":34}';
uploadMessage(messageTEST); 
*/
