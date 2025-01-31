#include <iostream>
#include <string>
#include <mqtt/async_client.h>
#include <curl/curl.h>
#include <jsoncpp/json/json.h>
#include <chrono>
#include <ctime>
#include <thread>
#include <iomanip>

const std::string SERVER_ADDRESS("tcp://localhost:1883");
const std::string CLIENT_ID("DataCollectorClient");
const std::string MACHINE_ID("machine_01");
const std::string SENSOR_ID_TEMPERATURE("sensor_temperature");
const std::string SENSOR_ID_HUMIDITY("sensor_humidity");
const int DATA_INTERVAL = 10; // em segundos

// Função para obter o timestamp atual em formato ISO 8601
std::string getCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    std::ostringstream oss;
    oss << std::put_time(std::gmtime(&now_time_t), "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

// Função para processar os dados da resposta HTTP e adicionar à string de origem
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* bufferptr) {
    ((std::string*)bufferptr)->append((char*)contents, size * nmemb);
    return size * nmemb;
}
// Função para pegar os dados da API OpenWeatherMap
std::string getWeatherData(const std::string& apiKey, const std::string& city, const std::string& country) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();

    //verifica a inicialização do manipulador
    if (curl) {
        std::string url = "http://api.openweathermap.org/data/2.5/weather?q=" + city + "," + country + "&appid=" + apiKey + "&units=metric";
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        
        res = curl_easy_perform(curl);
        
        if (res != CURLE_OK) {
            std::cerr << "CURL request failed: " << curl_easy_strerror(res) << std::endl;
        }

        curl_easy_cleanup(curl);
    }

    //finaliza a biblioteca cURL
    curl_global_cleanup();
    //testa readBUFFER
    //std::cout << readBuffer << std::endl;
    return readBuffer;
}

// Função para processar os dados JSON e extrair temperatura e umidade
void processWeatherData(const std::string& data, float& temperature, float& humidity) {

    Json::CharReaderBuilder reader;
    Json::Value root;
    std::istringstream s(data);
    std::string errs;

    if (Json::parseFromStream(reader, s, &root, &errs)) {
        temperature = root["main"]["temp"].asFloat();
        humidity = root["main"]["humidity"].asFloat();
    } else {
        std::cerr << "Error parsing JSON: " << errs << std::endl;
    }
}

void publishInitialMessage(mqtt::async_client& client) {
    Json::Value root;
    root["machine_id"] = MACHINE_ID;
    
    Json::Value sensor1;
    sensor1["sensor_id"] = SENSOR_ID_TEMPERATURE;
    sensor1["data_type"] = "float";
    sensor1["data_interval"] = DATA_INTERVAL;

    Json::Value sensor2;
    sensor2["sensor_id"] = SENSOR_ID_HUMIDITY;
    sensor2["data_type"] = "float";
    sensor2["data_interval"] = DATA_INTERVAL;

    root["sensors"][0] = sensor1;
    root["sensors"][1] = sensor2;

    Json::StreamWriterBuilder writer;
    std::string message = Json::writeString(writer, root);
    std::cout << message << std::endl;
    mqtt::message_ptr msg = mqtt::make_message("/sensor_monitors", message);
    client.publish(msg)->wait_for(std::chrono::seconds(10));
}

int main(int argc, char* argv[]) {

    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
    mqtt::connect_options connOpts;
    mqtt::token_ptr conntok = client.connect(connOpts);
    conntok->wait();

    publishInitialMessage(client);

    while (true) {
        // Pegando os dados da API
        std::string weatherData = getWeatherData("21309ecc4422778de48b2f48e31143cb", "Belo%20Horizonte", "BR");
        //std::string weatherData = getWeatherData("21309ecc4422778de48b2f48e31143cb", "Calama", "CL");
        //std::string weatherData = getWeatherData("21309ecc4422778de48b2f48e31143cb", "Sidney", "AUS");
        //std::string weatherData = getWeatherData("21309ecc4422778de48b2f48e31143cb", "Beijing", "CN");
        //std::string weatherData = getWeatherData("21309ecc4422778de48b2f48e31143cb", "London", "GB");
        
        if (!weatherData.empty()) {
            float temperature = 0.0f;
            float humidity = 0.0f;
            
            processWeatherData(weatherData, temperature, humidity);

            Json::Value tempMsg;
            tempMsg["timestamp"] = getCurrentTimestamp();
            tempMsg["value"] = temperature;

            Json::Value humMsg;
            humMsg["timestamp"] = getCurrentTimestamp();
            humMsg["value"] = humidity;

            Json::StreamWriterBuilder writer;
            std::string tempMessage = Json::writeString(writer, tempMsg);
            std::string humMessage = Json::writeString(writer, humMsg);
            //testa publicação
            std::cout << "temperature: " << tempMessage << std::endl << "umidity: " << humMessage << std::endl; 
            client.publish(mqtt::make_message("/sensors/" + MACHINE_ID + "/" + SENSOR_ID_TEMPERATURE, tempMessage))->wait_for(std::chrono::seconds(10));
            client.publish(mqtt::make_message("/sensors/" + MACHINE_ID + "/" + SENSOR_ID_HUMIDITY, humMessage))->wait_for(std::chrono::seconds(10));
            
            std::cout << "Published temperature and humidity. "<< std::endl;
        }

        std::this_thread::sleep_for(std::chrono::seconds(DATA_INTERVAL));
    }

    client.disconnect()->wait();

    return 0;
}
