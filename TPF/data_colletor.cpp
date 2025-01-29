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
const int DATA_INTERVAL = 10; // 30 minutos em segundos

// Função para obter o timestamp atual em formato ISO 8601
std::string getCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    std::ostringstream oss;
    oss << std::put_time(std::gmtime(&now_time_t), "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

// Função para processar os dados da resposta HTTP e adicionar à string de origem
// bufferptr aponta para o buffer onde os dados serao armazenados (readBuffer)
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* bufferptr) {
    ((std::string*)bufferptr)->append((char*)contents, size * nmemb);
    return size * nmemb;
}
// Função para pegar os dados da API OpenWeatherMap, chave, cidade, país
std::string getWeatherData(const std::string& apiKey, const std::string& city, const std::string& country) {
    //ponteiro para manipular o cliente http, biblioteca cURL
    CURL* curl;
    //codigo de retorno de falha ou sucesso
    CURLcode res;
    //armazena os dados recebidos da resposta http da api
    std::string readBuffer;

    //inicializa a biblioteca cURL no modo default
    curl_global_init(CURL_GLOBAL_DEFAULT);
    //inicializa o manipulador cURL para executar a solicitação http
    curl = curl_easy_init();

    //verifica a inicialização do manipulador
    if (curl) {
        //url da API, metric: graus celsius
        std::string url = "http://api.openweathermap.org/data/2.5/weather?q=" + city + "," + country + "&appid=" + apiKey + "&units=metric";
        //configura o manipulador para usar a url gerada (c_str converte string para const char*)
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        //Define uma função de retorno (WriteCallback) para ser chamada sempre que os dados forem recebidos da resposta HTTP.
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        //define o destino dos dados de writeCallBack
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        
        //executa a solicitação http e armazena em res o codigo de sucesso
        res = curl_easy_perform(curl);
        
        //Verifica se a solicitação foi bem-sucedida.
        if (res != CURLE_OK) {
            std::cerr << "CURL request failed: " << curl_easy_strerror(res) << std::endl;
        }

        //libera o manipulador
        curl_easy_cleanup(curl);
    }

    //finaliza a biblioteca cURL
    curl_global_cleanup();
    //testa readBUFFER
    std::cout << readBuffer << std::endl;
    //retorna os dados JSON da api
    return readBuffer;
}

// Função para processar os dados JSON e extrair temperatura e umidade
//data-> referencia para a string que contem os dados json da api
//temperature-> referencia para a variavel onde a temperatura sera armazenada
//humidity -> referencia para a variavel onde a umidade sera armazenada
void processWeatherData(const std::string& data, float& temperature, float& humidity) {
    //inicializa um objeto charReaderBuilderusado para configurar e criar um leitor de JSON
    Json::CharReaderBuilder reader;
    //estrutura de dados, objeto json generico (root = base) para armazenar dados extraidos da string json, acessado como um map
    Json::Value root;
    //converte a string data em um formato que pode ser lido sequencialmente, como se fosse um arquivo de entrada.
    std::istringstream s(data);
    //armazena mensagem de erro
    std::string errs;
    //converte a string s em um objeto value root, reader configura o parser
    if (Json::parseFromStream(reader, s, &root, &errs)) {
        //acessa as propriedades dentro do root e converte para float, salvando nas variaveis de referencia
        temperature = root["main"]["temp"].asFloat();
        humidity = root["main"]["humidity"].asFloat();
    } else {
        std::cerr << "Error parsing JSON: " << errs << std::endl;
    }
}

// Função para publicar a mensagem inicial
void publishInitialMessage(mqtt::async_client& client) {
    Json::Value root;
    root["machine_id"] = MACHINE_ID;
    
    //cria sensores
    Json::Value sensor1;
    sensor1["sensor_id"] = SENSOR_ID_TEMPERATURE;
    sensor1["data_type"] = "float";
    sensor1["data_interval"] = DATA_INTERVAL;

    Json::Value sensor2;
    sensor2["sensor_id"] = SENSOR_ID_HUMIDITY;
    sensor2["data_type"] = "float";
    sensor2["data_interval"] = DATA_INTERVAL;

    //adiciona cada sensor ao campo sensors (array)
    root["sensors"][0] = sensor1;
    root["sensors"][1] = sensor2;

    //inicializa um ojeto streamWriterBuilder que converte o ojbeto json root em string formato json
    Json::StreamWriterBuilder writer;
    //convertendo... e armazenando em message
    std::string message = Json::writeString(writer, root);
    //testa mensagem inicial
    std::cout << message << std::endl;
    //cria a mensagem mqtt no topico /sensor_monitors, conteudo: message, e armazena em msg
    mqtt::message_ptr msg = mqtt::make_message("/sensor_monitors", message);
    //publica a msg no topico pelo cliente client (data_colletor)
    client.publish(msg)->wait_for(std::chrono::seconds(10));
}

int main(int argc, char* argv[]) {
    //inicializa o cliente mqtt assíncrono
    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);

    // Conectando ao broker
    //objeto de opções de conexão para configurar a conexão entre o client e o broker mqtt
    mqtt::connect_options connOpts;
    //inicia a conexão com o broker mqtt
    mqtt::token_ptr conntok = client.connect(connOpts);
    //bloqueia a execução até estabelezar a conexão
    conntok->wait();

    // Publicando mensagem inicial
    publishInitialMessage(client);

    while (true) {
        // Pegando os dados da API
        std::string weatherData = getWeatherData("21309ecc4422778de48b2f48e31143cb", "Belo%20Horizonte", "BR");
        //std::string weatherData = getWeatherData("21309ecc4422778de48b2f48e31143cb", "Beijing", "CN");
        //std::string weatherData = getWeatherData("21309ecc4422778de48b2f48e31143cb", "London", "GB");
        //std::string weatherData = getWeatherData("21309ecc4422778de48b2f48e31143cb", "New%20York", "US");
        
        //verifica se a string está vazia
        if (!weatherData.empty()) {
            //declara as variaveis onde os dados serao armazenados
            float temperature = 0.0f;
            float humidity = 0.0f;
            
            // Processando os dados
            processWeatherData(weatherData, temperature, humidity);

            // Criando mensagens JSON no formato especificado
            Json::Value tempMsg;
            tempMsg["timestamp"] = getCurrentTimestamp();
            tempMsg["value"] = temperature;

            Json::Value humMsg;
            humMsg["timestamp"] = getCurrentTimestamp();
            humMsg["value"] = humidity;

            //converte o objeto json em string formato json
            Json::StreamWriterBuilder writer;
            std::string tempMessage = Json::writeString(writer, tempMsg);
            std::string humMessage = Json::writeString(writer, humMsg);
            //testa publicação
            std::cout << "temperature: " << tempMessage << std::endl << "umidity: " << humMessage << std::endl; 
            // Publicando as mensagens no MQTT
            client.publish(mqtt::make_message("/sensors/" + MACHINE_ID + "/" + SENSOR_ID_TEMPERATURE, tempMessage))->wait_for(std::chrono::seconds(10));
            client.publish(mqtt::make_message("/sensors/" + MACHINE_ID + "/" + SENSOR_ID_HUMIDITY, humMessage))->wait_for(std::chrono::seconds(10));
            
            std::cout << "Published temperature and humidity. "<< std::endl;
        }

        // Aguardar antes de coletar novamente
        std::this_thread::sleep_for(std::chrono::seconds(DATA_INTERVAL));
    }

    // Desconectando
    client.disconnect()->wait();

    return 0;
}
