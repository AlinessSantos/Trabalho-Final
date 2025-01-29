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

//onde o cliente se conectará, broker roda ocalmente na porta 1883
const std::string SERVER_ADDRESS("tcp://localhost:1883");
//ID único do cliente MQTT
const std::string CLIENT_ID("DataProcessorClient");
//máquina que está enviando os dados, deveríamos pegar do json??
const std::string MACHINE_ID("machine_01");
//intervalo de tempo entre verificações de sensores inativos
const int DATA_INTERVAL = 10; // em segundos

// Conexão com banco de dados SQLite
sqlite3* db;
//retorna um int de sucesso ou erro na criação da tabela
int createTables() {
    //string de comando em SQL para criar a tabela, R"()" é string raw de c++
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
    //armazena mensagens de erro
    char* errorMessage;
    //executa o comando char de criação de tabela em SQL, erro armazenado na variavel
    if (sqlite3_exec(db, createSensorsTableSQL, 0, 0, &errorMessage) != SQLITE_OK) {
        //mensagem de erro é exibida no stderr (fluxo de erro padrão)
        std::cerr << "Error creating sensor_data table: " << errorMessage << std::endl;
        //libera memória da variavel do tipo ponteiro
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
    //Quantidade de períodos (T=10s???) sem receber dados do sensor
    int missed_periods = 0;
};

// Classe para gerenciar alarmes
// retorna um inteiro de sucesso ou falha na inserção de uma nova leitura de sensor na tabela sensor_data
int insertSensorData(const std::string& machineId, const std::string& sensorId, float value, const std::string& timestamp) {
    //Comando em SQL para interir o registro na tabela (? são placeholders = espaço reservado)
    std::string sql = "INSERT INTO sensor_data (machine_id, sensor_id, value, timestamp) VALUES (?, ?, ?, ?);";
    //Ponteiro statement = uma declaração SQL compilada que será executada no SQLite.
    sqlite3_stmt* stmt;
    
    //Compila string SQL no stmt a ser executada no banco db (c_str converte string para const char*)
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0) != SQLITE_OK) {
        std::cerr << "Error preparing statement: " << sqlite3_errmsg(db) << std::endl;
        return -1;
    }
    
    //Comandos que substituem os placeholder "?" pelos valores (-1 calcula o tamanho da string automaticamente)
    sqlite3_bind_text(stmt, 1, machineId.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, sensorId.c_str(), -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 3, value);
    sqlite3_bind_text(stmt, 4, timestamp.c_str(), -1, SQLITE_STATIC);
    
    //executa stmt e retorna se foi bem sucedida
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        std::cerr << "Error executing statement: " << sqlite3_errmsg(db) << std::endl;
        //libera memoria do ponteiro
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
        //std::cout << "processSensorData()" << std::endl;
        //std::cout<< "sensor ID: " << sensorId << "-  value: " << value << std::endl;
        //garante que apenas uma thread por vez acesse o dataMap
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

    // Função para verificar sensores inativos
    void checkInactiveSensors() {
        //std::cout << "checkInactiveSensors()" << std::endl;
        std::lock_guard<std::mutex> lock(dataMutex);
        for (auto& entry : sensorDataMap) {
            SensorData& data = entry.second;
            data.missed_periods++;
            //std::cout << entry.second.value << ": " << entry.second.missed_periods << std::endl;
            // Se o sensor estiver inativo por 3 períodos, gerar alarme
            if (data.missed_periods >= 3) {
                std::cout << "ALARM: " + MACHINE_ID + ".alarms.inactive  SINCE: " + data.timestamp << std::endl;
                insertAlarm(MACHINE_ID, "inactive", data.timestamp);
            }
        }   
    }

};

// Função para processar os dados JSON recebidos
void processIncomingMessage(std::string& topic, const std::string& message, DataProcessor& processor) {
    //cria um objeto reader que constroi um parser json, que será utilizado para converter uma string em json value
    Json::CharReaderBuilder reader;
    //estrutura de dados, objeto json generico para armazenar dados extraidos da string json, acessado como um map
    Json::Value root;
    //converte a string message em um formato que pode ser lido sequencialmente, como se fosse um arquivo de entrada.
    std::istringstream s(message);
    //armazena mensagem de erro
    std::string errs;
    std::string sensorId;
    //std::cout<< "topic: " << topic << "- message: " << message << std::endl;
    if(topic.find("sensor_temperature")!=std::string::npos){
        sensorId = "sensor_temperature";
        //std::cout << "sensor ID processIncoming1: " << sensorId << std::endl;
    }
    if(topic.find("sensor_humidity")!=std::string::npos){
        sensorId = "sensor_humidity";
        //std::cout << "sensor ID processIncoming2: " << sensorId << std::endl;
    }
    
    //converte a string s em um objeto value root, reader configura o parser
    if (Json::parseFromStream(reader, s, &root, &errs)) {
        //std::cout << "anem id......"<< root["machine_id"].asString() << std::endl;
        //std::string sensorId;
        float value = root["value"].asFloat();
        std::string timestamp = root["timestamp"].asString();
        //std::cout << "processIncomingMessage() - " << sensorId<< std::endl;
        // Processa os dados do sensor
        if(sensorId.size()> 3) processor.processSensorData(sensorId, value, timestamp);
    } else {
        std::cerr << "Error parsing JSON: " << errs << std::endl;
    }
    
}

// Função para processar os alarmes
void processAlarms(DataProcessor& processor) {
    //std::cout << "processAlarms()" << std::endl;
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
        //std::cout << "message_arrived()" << std::endl;
        //tópico ao qual a mensagem foi publicada
        std::string topic = msg->get_topic();
        //std::cout <<"topic: "<< topic << std::endl;
        // conteúdo da mensagem
        std::string message = msg->get_payload_str();
        processIncomingMessage(topic, message, processor);   
    }
};

int main(int argc, char* argv[]) {
    // abrir/criar um aqrquivo de banco de dados 
    if (sqlite3_open("sensor_data.db", &db) != SQLITE_OK) {
        std::cerr << "Error opening SQLite database: " << sqlite3_errmsg(db) << std::endl;
        return -1;
    }

    // Criar as tabelas no banco de dados
    if (createTables() != 0) {
        return -1;
    }
    
    //inicializa o cliente mqtt assíncrono
    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
    //objeto de opções de conexão para configurar a conexão entre o client e o broker mqtt
    mqtt::connect_options connOpts;
    //inicia a conexão com o broker mqtt
    mqtt::token_ptr conntok = client.connect(connOpts);
    //bloqueia a execução até estabelezar a conexão
    conntok->wait();
    //std::cout << "conectou" << std::endl;
    DataProcessor processor(client);

    // Instanciando o callback handler
    CallbackHandler callbackHandler(processor);

    // Definindo o callback do cliente para lidar com eventos do mqtt
    client.set_callback(callbackHandler);

    // Inscrevendo-se nos tópicos (1-recebe pelo menos uma vez)
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