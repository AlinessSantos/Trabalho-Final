#include <iostream>
#include <string>
#include <mqtt/async_client.h>
#include <jsoncpp/json/json.h>
#include <chrono>
#include <thread>
#include <map>
#include <mutex>
#include <ctime>
#include <iomanip>
#include <sqlite3.h>

const std::string SERVER_ADDRESS("tcp://localhost:1883");
const std::string CLIENT_ID("DataProcessorClient");
const std::string MACHINE_ID("machine_01");
const int DATA_INTERVAL = 10; // 30 minutos em segundos

// Conexão com banco de dados SQLite
sqlite3* db;

int createTables() {
    const char* createSensorsTableSQL = R"(
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT,
            sensor_id TEXT,
            value REAL,
            timestamp TEXT
        );
    )";
    
    const char* createAlarmsTableSQL = R"(
        CREATE TABLE IF NOT EXISTS alarms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id TEXT,
            alarm_type TEXT,
            timestamp TEXT
        );
    )";
    
    char* errorMessage;
    if (sqlite3_exec(db, createSensorsTableSQL, 0, 0, &errorMessage) != SQLITE_OK) {
        std::cerr << "Error creating sensor_data table: " << errorMessage << std::endl;
        sqlite3_free(errorMessage);
        return -1;
    }
    
    if (sqlite3_exec(db, createAlarmsTableSQL, 0, 0, &errorMessage) != SQLITE_OK) {
        std::cerr << "Error creating alarms table: " << errorMessage << std::endl;
        sqlite3_free(errorMessage);
        return -1;
    }
    
    return 0;
}

// Estrutura para armazenar dados dos sensores
struct SensorData {
    float value;
    std::string timestamp;
    int missed_periods = 0;
};

// Classe para gerenciar alarmes
class AlarmManager {
public:
    void generateAlarm(const std::string& alarmMessage) {
        std::cout << "ALARM: " << alarmMessage << std::endl;
        // Persistir no banco de dados (simulação)
    }
};
int insertSensorData(const std::string& machineId, const std::string& sensorId, float value, const std::string& timestamp) {
    std::string sql = "INSERT INTO sensor_data (machine_id, sensor_id, value, timestamp) VALUES (?, ?, ?, ?);";
    sqlite3_stmt* stmt;
    
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0) != SQLITE_OK) {
        std::cerr << "Error preparing statement: " << sqlite3_errmsg(db) << std::endl;
        return -1;
    }
    
    sqlite3_bind_text(stmt, 1, machineId.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, sensorId.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 3, value);
    sqlite3_bind_text(stmt, 4, timestamp.c_str(), -1, SQLITE_STATIC);
    
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        std::cerr << "Error executing statement: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_finalize(stmt);
        return -1;
    }
    
    sqlite3_finalize(stmt);
    return 0;
}
int insertAlarm(const std::string& machineId, const std::string& alarmType, const std::string& timestamp) {
    std::string sql = "INSERT INTO alarms (machine_id, alarm_type, timestamp) VALUES (?, ?, ?);";
    sqlite3_stmt* stmt;
    
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0) != SQLITE_OK) {
        std::cerr << "Error preparing statement: " << sqlite3_errmsg(db) << std::endl;
        return -1;
    }
    
    sqlite3_bind_text(stmt, 1, machineId.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, alarmType.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, timestamp.c_str(), -1, SQLITE_STATIC);
    
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        std::cerr << "Error executing statement: " << sqlite3_errmsg(db) << std::endl;
        sqlite3_finalize(stmt);
        return -1;
    }
    
    sqlite3_finalize(stmt);
    return 0;
}
// Processamento de dados dos sensores
class DataProcessor {
private:
    mqtt::async_client& client;
    std::map<std::string, SensorData> sensorDataMap;
    AlarmManager alarmManager;
    std::mutex dataMutex;

public:
    DataProcessor(mqtt::async_client& mqttClient) : client(mqttClient) {}

    // Função para processar os dados de temperatura e umidade
    void processSensorData(const std::string& sensorId, float value, const std::string& timestamp) {
        std::cout << "processSensorData()" << std::endl;
        std::cout<< "sensor ID: " << sensorId << "-  value: " << value << std::endl;
        std::lock_guard<std::mutex> lock(dataMutex);
        //std::string sensorId;

        // Verifica se o sensor já existe, caso contrário, cria um novo
        if (sensorDataMap.find(sensorId) == sensorDataMap.end()) {
            sensorDataMap[sensorId] = {value, timestamp};
            return;
        }

        SensorData& data = sensorDataMap[sensorId];
        // Atualiza os dados do sensor
        data.value = value;
        data.timestamp = timestamp;
        data.missed_periods = 0;  // Reset missed periods ao receber novo dado

        // Persistir dados no banco SQLite
        insertSensorData(MACHINE_ID, sensorId, value, timestamp);
        // Verifica se o valor do sensor está dentro da faixa ideal
        if (sensorId == "sensor_temperature") {
            if (value > 20.0f && value < 40.0f) {
                alarmManager.generateAlarm(MACHINE_ID + ".alarms.temperature");
                insertAlarm(MACHINE_ID, "temperature", timestamp);
            }
        } else if (sensorId == "sensor_humidity") {
            if (value > 40.0f && value < 80.0f) {
                alarmManager.generateAlarm(MACHINE_ID + ".alarms.humidity");
                insertAlarm(MACHINE_ID, "humidity", timestamp);
            }
        }

        // Processamento de inatividade
        if (data.missed_periods >= 10) {
            alarmManager.generateAlarm(MACHINE_ID + ".alarms.inactive");
        }
        
    }

    // Função para verificar sensores inativos
    void checkInactiveSensors() {
        std::cout << "checkInactiveSensors()" << std::endl;
        std::lock_guard<std::mutex> lock(dataMutex);

        for (auto& entry : sensorDataMap) {
            SensorData& data = entry.second;
            data.missed_periods++;

            // Se o sensor estiver inativo por 10 períodos, gerar alarme
            if (data.missed_periods >= 10) {
                alarmManager.generateAlarm(MACHINE_ID + ".alarms.inactive");
                insertAlarm(MACHINE_ID, "inactive", data.timestamp);
            }
        }
        
    }

};

// Função para processar os dados JSON recebidos
void processIncomingMessage(std::string& topic, const std::string& message, DataProcessor& processor) {
    
    Json::CharReaderBuilder reader;
    Json::Value root;
    std::istringstream s(message);
    std::string errs;
    std::string sensorId;
    std::cout<< "topic: " << topic << "- message: " << message << std::endl;
    if(topic.find("sensor_temperature")!=std::string::npos){
        sensorId = "sensor_temperature";
        //std::cout << "sensor ID processIncoming1: " << sensorId << std::endl;
    }
    if(topic.find("sensor_humidity")!=std::string::npos){
        sensorId = "sensor_humidity";
        //std::cout << "sensor ID processIncoming2: " << sensorId << std::endl;
    }
    
    if (Json::parseFromStream(reader, s, &root, &errs)) {
        //std::cout << "anem id......"<< root["machine_id"].asString() << std::endl;
        //std::string sensorId;
        float value = root["value"].asFloat();
        std::string timestamp = root["timestamp"].asString();
        //std::cout << "processIncomingMessage() - " << sensorId<< std::endl;
        // Processa os dados do sensor
        processor.processSensorData(sensorId, value, timestamp);
    } else {
        std::cerr << "Error parsing JSON: " << errs << std::endl;
    }
    
}

// Função para processar os alarmes
void processAlarms(DataProcessor& processor) {
    std::cout << "processAlarms()" << std::endl;
    processor.checkInactiveSensors();
}

// Classe que implementa a interface mqtt::callback
class CallbackHandler : public mqtt::callback {
private:
    DataProcessor& processor;

public:
    CallbackHandler(DataProcessor& proc) : processor(proc) {}

    void message_arrived(mqtt::const_message_ptr msg) {
        std::cout << "message_arrived()" << std::endl;
        std::string topic = msg->get_topic();
        //std::cout <<"topic: "<< topic << std::endl;
        std::string payload = msg->get_payload_str();
        processIncomingMessage(topic, payload, processor);   
    }
};

int main(int argc, char* argv[]) {
    // Conectar ao banco de dados SQLite
    if (sqlite3_open("sensor_data.db", &db) != SQLITE_OK) {
        std::cerr << "Error opening SQLite database: " << sqlite3_errmsg(db) << std::endl;
        return -1;
    }

    // Criar as tabelas no banco de dados
    if (createTables() != 0) {
        return -1;
    }
    
    
    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
    mqtt::connect_options connOpts;
    mqtt::token_ptr conntok = client.connect(connOpts);
    conntok->wait();
    //std::cout << "conectou" << std::endl;
    DataProcessor processor(client);

    // Instanciando o callback handler
    CallbackHandler callbackHandler(processor);

    // Definindo o callback do cliente
    client.set_callback(callbackHandler);

    // Inscrevendo-se nos tópicos
    client.subscribe("/sensor_monitors", 1)->wait();
    client.subscribe("/sensors/" + MACHINE_ID + "/sensor_temperature", 1)->wait();
    client.subscribe("/sensors/" + MACHINE_ID + "/sensor_humidity", 1)->wait();

    // Loop para verificar sensores inativos e alarmes
    while (true) {
        processAlarms(processor);
        std::this_thread::sleep_for(std::chrono::seconds(DATA_INTERVAL));
    }

    // Desconectar do banco de dados e MQTT
    sqlite3_close(db);
    client.disconnect()->wait();

    return 0;
}