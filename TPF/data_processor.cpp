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

const std::string SERVER_ADDRESS("tcp://localhost:1883");
const std::string CLIENT_ID("DataProcessorClient");
const std::string MACHINE_ID("machine_01");
const int DATA_INTERVAL = 10; // 30 minutos em segundos

// Definição para alarmes
enum class AlarmType {
    Inactive,
    Temperature,
    Humidity
};

// Estrutura para armazenar dados dos sensores
struct SensorData {
    float value;
    std::string timestamp;
    int missed_periods = 0;
};

// Função para obter o timestamp atual em formato ISO 8601
std::string getCurrentTimestamp() {
    std::cout << "getCurrentTimestamp()" << std::endl;
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    struct tm tm_info;
    gmtime_r(&now_time_t, &tm_info);  // Converte para o formato UTC
    
    char buffer[100];
    strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &tm_info);  // Formato ISO 8601
    return std::string(buffer);
}

// Classe para gerenciar alarmes
class AlarmManager {
public:
    void generateAlarm(const std::string& alarmMessage) {
        std::cout << "ALARM: " << alarmMessage << std::endl;
        // Persistir no banco de dados (simulação)
    }
};

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
        std::lock_guard<std::mutex> lock(dataMutex);

        // Verifica se o sensor já existe, caso contrário, cria um novo
        if (sensorDataMap.find(sensorId) == sensorDataMap.end()) {
            sensorDataMap[sensorId] = {value, timestamp};
            return;
        }

        SensorData& data = sensorDataMap[sensorId];
        
        // Verifica se o valor do sensor está dentro da faixa ideal
        if (sensorId == "sensor_temperature") {
            if (value < 20.0f || value > 30.0f) {
                alarmManager.generateAlarm(MACHINE_ID + ".alarms.temperature");
            }
        } else if (sensorId == "sensor_humidity") {
            if (value < 40.0f || value > 80.0f) {
                alarmManager.generateAlarm(MACHINE_ID + ".alarms.humidity");
            }
        }

        // Atualiza os dados do sensor
        data.value = value;
        data.timestamp = timestamp;
        data.missed_periods = 0;  // Reset missed periods ao receber novo dado

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
            }
        }
        
    }

    // Função para processar a média móvel
    float calculateMovingAverage(const std::string& sensorId, float newValue) {
        std::cout << "calculateMovingAverage()" << std::endl;
        static std::map<std::string, std::vector<float>> sensorValues;

        // Adiciona o novo valor ao vetor de valores do sensor
        sensorValues[sensorId].push_back(newValue);

        // Limita o número de valores no vetor para calcular a média móvel
        if (sensorValues[sensorId].size() > 5) {
            sensorValues[sensorId].erase(sensorValues[sensorId].begin());
        }

        // Calcula a média
        float sum = 0;
        for (float val : sensorValues[sensorId]) {
            sum += val;
        }
        
        return sum / sensorValues[sensorId].size();
        
    }
};

// Função para processar os dados JSON recebidos
void processIncomingMessage(const std::string& topic, const std::string& message, DataProcessor& processor) {
    std::cout << "processIncomingMessage()" << std::endl;
    Json::CharReaderBuilder reader;
    Json::Value root;
    std::istringstream s(message);
    std::string errs;

    if (Json::parseFromStream(reader, s, &root, &errs)) {
        std::string sensorId = root["sensor_id"].asString();
        float value = root["value"].asFloat();
        std::string timestamp = root["timestamp"].asString();

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
        std::string payload = msg->get_payload_str();
        processIncomingMessage(topic, payload, processor);   
    }

    // Removido o 'override' para evitar o erro de não sobrecarga correta
    void connected(const std::string& cause) {
        std::cout << "Connected to MQTT broker" << std::endl;
        
    }

    // Removido o 'override' para evitar o erro
    void disconnected(const std::string& cause) {
        std::cout << "Disconnected from MQTT broker" << std::endl;
    }

    // Removido o 'override' para evitar o erro
    void delivery_complete(mqtt::delivery_token_ptr tok) {
        std::cout << "Delivery complete" << std::endl;
    }
};

int main(int argc, char* argv[]) {
    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
    mqtt::connect_options connOpts;
    mqtt::token_ptr conntok = client.connect(connOpts);
    conntok->wait();
    std::cout << "conectou" << std::endl;
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

    // Desconectando
    client.disconnect()->wait();

    return 0;
}