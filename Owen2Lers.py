import json
import time
import requests
import logging
from datetime import datetime, timezone

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')

# Загружаем конфигурационный файл
def load_config(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

# Аутентификация в OwenCloud и получение токена
def authenticate(login, password):
    auth_url = f"https://api.owencloud.ru/v1/auth/open"
    payload = {"login": login, "password": password}
    response = requests.post(auth_url, json=payload)

    if response.status_code == 200:
        data = response.json()
        if data.get("error_status") == 0:
            return data.get("token")
        else:
            logging.error(f"Ошибка при аутентификации в OwenCloud: {data}")
            raise Exception(f"Ошибка при аутентификации в OwenCloud: {data}")
    else:
        logging.error(f"Ошибка при аутентификации в OwenCloud: {response.status_code}, {response.text}")
        raise Exception(f"Ошибка при аутентификации в OwenCloud: {response.status_code}, {response.text}")

# Получаем текущие данные из OwenCloud
def fetch_current_data(token, parameter_ids):
    data_url = f"https://api.owencloud.ru/v1/parameters/last-data"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"ids": parameter_ids}

    response = requests.post(data_url, json=payload, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Ошибка при получении текущих данных: {response.status_code}, {response.text}")
        raise Exception(f"Ошибка при получении текущих данных: {response.status_code}, {response.text}")

# Записываем данные в ЛЭРС
def send_data_to_lers(server_url, token, lers_measurepoint_id, consumption_data):
    try:
        payload = {
            "data": {
                "dataType": "Current",
                "consumption": consumption_data
            }
        }

        # Отправляем PUT запрос
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{server_url}/api/v1/Data/MeasurePoints/{lers_measurepoint_id}/Consumption/CurrentArchive"
        response = requests.put(url, json=payload, headers=headers)

        if response.status_code == 200:
            logging.info(f"Данные успешно отправлены на сервер ЛЭРС для точки учёта {lers_measurepoint_id}")
        else:
            logging.error(f"Ошибка при отправке данных на сервер ЛЭРС: {response.status_code}, {response.text}")
    except Exception as e:
        logging.exception(f"Исключение при отправке данных на сервер ЛЭРС: {e}")


def main():
    config = load_config("config.json")

    login = config["login"]
    password = config["password"]
    lers_server_url = config["lers_server_url"]
    lers_token = config["lers_token"]
    send_interval = config["send_interval"]
    parameters_config = config["parameters"]

    # Собираем все parameter_ids для одного запроса
    all_parameter_ids = []
    for lers_group in parameters_config:
        for param in lers_group["parameters"]:
            all_parameter_ids.append(param["owen_parameter_id"])

    last_timestamps = {param_id: None for param_id in all_parameter_ids}

    try:
        token = authenticate(login, password)
        logging.info("Успешная аутентификация в OwenCloud.")

        while True:
            try:
                data = fetch_current_data(token, all_parameter_ids)
                
                # Форматируем вывод данных в лог
                formatted_data = "id параметра; дата; значение\n"
                for item in data:
                    item_id = item["id"]
                    if item["values"]:
                        timestamp = item["values"][0]["d"]
                        value = item["values"][0]["v"]
                        date_time = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S %d.%m.%y')
                        formatted_data += f"{item_id}; {date_time}; {value}\n"
                    else:
                        formatted_data += f"{item_id}; None; None\n"
                
                logging.info("Получены данные от OwenCloud:\n%s", formatted_data)

                # Инициализируем пустой словарь для хранения данных
                data_map = {}

                # Проходим по каждому элементу в списке `data`
                for item in data:
                    # Преобразуем значение `id` в строковый тип и присваиваем переменной `item_id`
                    item_id = str(item["id"])

                    # Проверяем, есть ли значение для ключа "values" в текущем элементе (item)
                    if item["values"]:
                        # Если значение есть, извлекаем первый элемент списка "values"
                        # и присваиваем переменной `data_value`
                        data_value = item["values"][0]
                    else:
                        # Если значение отсутствует, присваиваем `data_value` значение `None`
                        data_value = None

                    # Добавляем значение в словарь `data_map` с ключом `item_id`
                    data_map[item_id] = data_value


                for lers_group in parameters_config:
                    lers_measurepoint_id = lers_group["lers_measurepoint_id"]
                    consumption_data = []

                    for param in lers_group["parameters"]:
                        owen_parameter_id = param["owen_parameter_id"]
                        lers_data_parameter = param["lers_dataParameter"]
                        owen_value = data_map.get(owen_parameter_id)

                        if owen_value:
                            timestamp = owen_value["d"]
                            value = owen_value["v"]

                            # Проверяем изменение timestamp
                            if last_timestamps[owen_parameter_id] != timestamp:
                                last_timestamps[owen_parameter_id] = timestamp

                                date_time = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

                                # Готовим consumption_data
                                consumption_entry = next(
                                    (entry for entry in consumption_data if entry["dateTime"] == date_time), None)
                                if consumption_entry is None:
                                    consumption_entry = {
                                        "dateTime": date_time,
                                        "values": []
                                    }
                                    consumption_data.append(consumption_entry)

                                consumption_entry["values"].append({
                                    "dataParameter": lers_data_parameter,
                                    "value": value
                                })
                            else:
                                logging.info(f"Нет новых данных для параметра {owen_parameter_id}, пропускаем.")
                        else:
                            logging.warning(f"Данные не получены для параметра {owen_parameter_id}.")

                    if consumption_data:
                        send_data_to_lers(lers_server_url, lers_token, lers_measurepoint_id, consumption_data)
                    else:
                        logging.info(f"Нет новых данных для отправки в ЛЭРС для точки учёта {lers_measurepoint_id}.")

            except Exception as e:
                logging.exception("Ошибка при получении или отправке данных: %s", e)

            time.sleep(send_interval)  # ждём указанное время

    except Exception as e:
        logging.exception("Ошибка: %s", e)

if __name__ == "__main__":
    main()
