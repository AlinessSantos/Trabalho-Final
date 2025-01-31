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
const int DATA_INTERVAL = 10; // em segundos

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
    if(alarmType !="inactive"){ 
        std::cout << "ALARM: " + MACHINE_ID + ".alarms." + alarmType + "  TIME: " + timestamp << std::endl;
    }
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
    std::mutex dataMutex;

public:
    DataProcessor(mqtt::async_client& mqttClient) : client(mqttClient) {}

    // Função para processar os dados de temperatura e umidade
    void processSensorData(const std::string& sensorId, float value, const std::string& timestamp) {

        std::lock_guard<std::mutex> lock(dataMutex);

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

        insertSensorData(MACHINE_ID, sensorId, value, timestamp);
 
        if (sensorId == "sensor_temperature") {
            if (value > 20.0f && value < 26.0f) {
                insertAlarm(MACHINE_ID, "good_temperature", timestamp);
            }
            if (value < 20.0f) {
                insertAlarm(MACHINE_ID, "low_temperature", timestamp);
            }
            if (value > 26.0f) {
                insertAlarm(MACHINE_ID, "high_temperature", timestamp);
            }
        } else if (sensorId == "sensor_humidity") {
            if (value > 40.0f && value < 60.0f) {
                insertAlarm(MACHINE_ID, "good_humidity", timestamp);
            }
            if (value < 40.0f) {
                insertAlarm(MACHINE_ID, "low_humidity", timestamp);
            }
            if (value > 60.0f) {
                insertAlarm(MACHINE_ID, "high_humidity", timestamp);
            }
        }
        
    }

    void checkInactiveSensors() {
        std::lock_guard<std::mutex> lock(dataMutex);
        for (auto& entry : sensorDataMap) {
            SensorData& data = entry.second;
            data.missed_periods++;

            // Se o sensor estiver inativo por 2 períodos, gerar alarme
            if (data.missed_periods >= 3) {
                std::cout << "ALARM: " + MACHINE_ID + ".alarms.inactive  SINCE: " + data.timestamp << std::endl;
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

    if(topic.find("sensor_temperature")!=std::string::npos){
        sensorId = "sensor_temperature";
    }
    if(topic.find("sensor_humidity")!=std::string::npos){
        sensorId = "sensor_humidity";
    }
    
    if (Json::parseFromStream(reader, s, &root, &errs)) {
        float value = root["value"].asFloat();
        std::string timestamp = root["timestamp"].asString();

        // Processa os dados do sensor
        if(sensorId.size()> 3) processor.processSensorData(sensorId, value, timestamp);
    } else {
        std::cerr << "Error parsing JSON: " << errs << std::endl;
    }
    
}

void processAlarms(DataProcessor& processor) {
    processor.checkInactiveSensors();
}

// Subclasse que implementa a interface mqtt::callback (interface de retorno 
// de chamada do cliente mqtt para processar eventos, mensagens, conexão, etc)
class CallbackHandler : public mqtt::callback {
private:
    DataProcessor& processor;

public:
    CallbackHandler(DataProcessor& proc) : processor(proc) {}

    // chamado automaticamente quando uma mensagem (ponteiro msg para a mensagem) é recebida
    void message_arrived(mqtt::const_message_ptr msg) {

        std::string topic = msg->get_topic();
        std::string message = msg->get_payload_str();
        processIncomingMessage(topic, message, processor);   
    }
};

int main(int argc, char* argv[]) {
    // abrir/criar um arquivo de banco de dados 
    if (sqlite3_open("sensor_data.db", &db) != SQLITE_OK) {
        std::cerr << "Error opening SQLite database: " << sqlite3_errmsg(db) << std::endl;
        return -1;
    }

    if (createTables() != 0) {
        return -1;
    }
    
    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
    mqtt::connect_options connOpts;
    mqtt::token_ptr conntok = client.connect(connOpts);
    conntok->wait();
    DataProcessor processor(client);

    CallbackHandler callbackHandler(processor);

    client.set_callback(callbackHandler);

    client.subscribe("/sensor_monitors", 1)->wait();
    client.subscribe("/sensors/" + MACHINE_ID + "/sensor_temperature", 1)->wait();
    client.subscribe("/sensors/" + MACHINE_ID + "/sensor_humidity", 1)->wait();

    while (true) {
        processAlarms(processor);
        std::this_thread::sleep_for(std::chrono::seconds(DATA_INTERVAL));
    }

    sqlite3_close(db);
    client.disconnect()->wait();

    return 0;
}