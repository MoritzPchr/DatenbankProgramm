const mqtt = require('mqtt');
const mysql = require('mysql2');

// MQTT Client konfigurieren
const client = mqtt.connect('mqtt://broker.hivemq.com'); // Beispielbroker

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
    client.subscribe('sensor/temperature', (err) => {
        if (err) {
            console.error('Subscription error:', err);
        } else {
            console.log('Subscribed to topic: sensor/temperature');
        }
    });
});

// Nachricht empfangen und in die Datenbank speichern
client.on('message', (topic, message) => {
    console.log(`Received message: ${message.toString()} on topic: ${topic}`);

    const sql = `INSERT INTO sensor_data (topic, message) VALUES (?, ?)`;
    db.query(sql, [topic, message.toString()], (err, results) => {
        if (err) {
            console.error('Error inserting data:', err.message);
            return;
        }
        console.log(`Data inserted with ID: ${results.insertId}`);
    });
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
