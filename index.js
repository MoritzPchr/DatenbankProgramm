const mqtt = require('mqtt');
const mysql = require('mysql2');
const Ajv = require('ajv');
const ajv = new Ajv();

// MQTT Client konfigurieren
//const client = mqtt.connect('mqtt://broker.hivemq.com'); // Beispeilsbroker IP
//const client = mqtt.connect('mqtt://192.10.10.10');
const client = mqtt.connect('mqtt://192.168.1.29:8883'); //--> mit Angabe des Portes


// Verbindung zur MySQL-Datenbank auf localhost (XAMPP)
const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',           // Standardbenutzername für XAMPP
    password: '',           // Standardpasswort für XAMPP ist leer, falls geändert, anpassen
    database: 'diplomarbeit'   // Name der bestehenden Datenbank
});

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
    console.log('Connected to MQTT broker');
    client.subscribe('sensor/pm', (err) => { //Topic angeben
        if (err) {
            console.error('Subscription error:', err);
        } else {
            console.log('Subscribed to topic.');
        }
    });
});

// Nachricht empfangen
client.on('message', (topic, message) => {
    console.log(`Received message: ${message.toString()} on topic: ${topic}`);
    uploadMessage(message);
});

// Handle process exit to close DB connection
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
        const parsedMessage = JSON.parse(message.toString());

        //JSON-Kontrolle:
        if (!validateMessage(parsedMessage)) {
            console.error('Ungültiges JSON-Format:', parsedMessage);
            return;
        }

        // Extrahiere die Werte aus dem JSON
        let { Id, PM1_0, PM2_5, PM10 } = parsedMessage;

        // Überprüfen ob Client existiert:
        const clientExists = await checkClient(Id);
        
        if (clientExists) {
            console.log("Client vorhanden");

            // SQL-Abfrage zum Einfügen der Daten in die Datenbank
            let sql = `INSERT INTO feinstaubwert (\`PM1.0\`, \`PM2.5\`, PM10, ClientID) VALUES (?, ?, ?, ?)`;
            db.query(sql, [PM1_0, PM2_5, PM10, Id], (err, results) => {
                if (err) {
                    console.error('Fehler beim Einfügen der Daten:', err.message);
                    return;
                }
                console.log(`Daten eingefügt mit ID: ${results.insertId}`);
            });
        } else {
            console.log("Client nicht vorhanden!");
        }
    } catch (err) {
        console.error('Fehler beim Verarbeiten der Nachricht:', err.message);
    }
}

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

// JSON-Schema für die erwarteten Felder
const schema = {
    type: "object",
    properties: {
        Id: { type: "string", pattern: "^[0-9]+$" }, // ID als String, nur numerische Zeichen erlaubt
        PM1_0: { type: "number" },
        PM2_5: { type: "number" },
        PM10: { type: "number" }
    },
    required: ["Id", "PM1_0", "PM2_5", "PM10"],
    additionalProperties: false // Blockiert unerwartete Felder
};

function validateMessage(data) {
    const validate = ajv.compile(schema);
    const valid = validate(data);
    if (!valid) {
        console.error('JSON-Validierungsfehler:', validate.errors);
        return false;
    }
    console.log("Client gültig!")
    return true;
}

//-----------------------------------------------------------------------------------
//XXXXXXXXXX
//Zur Simulation:
//XXXXXXXXXXX
const messageTEST = '{"Id":"1","PM1_0":0,"PM2_5":0,"PM10":34}';
uploadMessage(messageTEST);
