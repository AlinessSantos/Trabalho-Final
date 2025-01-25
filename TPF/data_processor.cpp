#include <iostream>
#include <fstream>
#include <map>
#include <set>
#include <chrono>
#include <thread>
#include <ctime>
#include <jsoncpp/json/json.h>
#include <mqtt/async_client.h>
#include <iomanip>

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"
#define GRAPHITE_HOST "graphite"
#define GRAPHITE_PORT 2003
#define INACTIVITY_LIMIT 10 // Limite de inatividade em períodos

using namespace std;

// Estruturas para armazenar os dados e os últimos timestamps
std::map<std::string, std::string> last_timestamps;
std::set<std::string> active_sensors;

void post_metric(const std::string& metric_path, const std::string& timestamp_str, float value) {
    std::ofstream graphite_stream;
    graphite_stream.open("/dev/null", std::ios::app); // Ajustar para Graphite real
    if (graphite_stream.is_open()) {
        graphite_stream << metric_path << " " << value << " " << timestamp_str << std::endl;
        graphite_stream.close();
    } else {
        std::cerr << "Error: Unable to connect to Graphite." << std::endl;
    }
}

std::vector<std::string> split(const std::string &str, char delim) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}

bool is_outlier(float value, float lower, float upper) {
    return (value < lower || value > upper);
}

void check_inactivity() {
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);

    for (const auto& sensor : active_sensors) {
        auto last_time = std::chrono::system_clock::from_time_t(std::stol(last_timestamps[sensor]));
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - last_time).count();

        if (duration > INACTIVITY_LIMIT * 10) {
            std::string metric_path = sensor + ".alarms.inactive";
            post_metric(metric_path, std::to_string(now_time_t), 1);
            std::cout << "Alarme de inatividade disparado para " << sensor << std::endl;
        }
    }
}

int main(int argc, char* argv[]) {
    std::string clientId = "DataProcessorClient";
    mqtt::async_client client(BROKER_ADDRESS, clientId);

    class callback : public virtual mqtt::callback {
    public:
        void message_arrived(mqtt::const_message_ptr msg) override {
            json::JSON payload = json::JSON::Load(msg->get_payload());
            std::string topic = msg->get_topic();
            auto topic_parts = split(topic, '/');

            std::string machine_id = topic_parts[2];
            std::string sensor_id = topic_parts[3];
            std::string full_sensor_id = machine_id + "." + sensor_id;

            active_sensors.insert(full_sensor_id);
            last_timestamps[full_sensor_id] = payload["timestamp"].ToString();

            float value = payload["value"].ToFloat();
            post_metric(full_sensor_id + ".data", payload["timestamp"].ToString(), value);

            // Verificação de alarmes
            if (sensor_id == "sensor_temperature" && is_outlier(value, 19.0, 32.0)) {
                post_metric(machine_id + ".alarms.temperature_outlier", payload["timestamp"].ToString(), value);
                std::cout << "Alarme de temperatura disparado." << std::endl;
            }

            if (sensor_id == "sensor_humidity" && is_outlier(value, 40.0, 80.0)) {
                post_metric(machine_id + ".alarms.humidity_outlier", payload["timestamp"].ToString(), value);
                std::cout << "Alarme de umidade disparado." << std::endl;
            }
        }
    };

    callback cb;
    client.set_callback(cb);

    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts);
        client.subscribe("/sensors/#", QOS);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    while (true) {
        check_inactivity();
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    return EXIT_SUCCESS;
}
