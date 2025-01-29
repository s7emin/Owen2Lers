import json
import requests

# ANSI escape sequences for colors
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'

def authenticate(login, password):
    auth_url = "https://api.owencloud.ru/v1/auth/open"
    payload = {"login": login, "password": password}
    response = requests.post(auth_url, json=payload)

    if response.status_code == 200:
        data = response.json()
        if data.get("error_status") == 0:
            token = data.get("token")
            name = data.get("name")
            surname = data.get("surname")
            company_name = data.get("company_name")
            return token, name, surname, company_name
        else:
            raise Exception(f"Ошибка при аутентификации в OwenCloud: {data}")
    else:
        raise Exception(f"Ошибка при аутентификации в OwenCloud: {response.status_code}, {response.text}")

def test_lers_connection(server_url):
    try:
        response = requests.get(f"{server_url}/api/v1/ServerInfo")
        if response.status_code == 200:
            data = response.json()
            version = data.get("version")
            print(f"{Colors.GREEN}Подключение по указанному URL установлено, версия сервера ЛЭРС УЧЕТ: {version}{Colors.RESET}")
            return True
        else:
            raise Exception(f"Ошибка при подключении к серверу ЛЭРС: {response.status_code}, {response.text}")
    except Exception as e:
        raise Exception(f"Ошибка при подключении к серверу ЛЭРС: {e}")

def test_lers_token(server_url, token):
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(f"{server_url}/api/v1/Login/Current", headers=headers)
        if response.status_code == 200:
            data = response.json()
            display_name = data.get("account", {}).get("displayName")
            permissions = data.get("permissions", [])
            print(f"{Colors.GREEN}Успешное подключение к серверу ЛЭРС. Пользователь: {display_name}{Colors.RESET}")
            if "saveData" not in permissions:
                print(f"{Colors.RED}Предупреждение: У учетной записи нет разрешения на импорт данных.{Colors.RESET}")
            return True
        else:
            raise Exception(f"Ошибка при подключении к серверу ЛЭРС с токеном: {response.status_code}, {response.text}")
    except Exception as e:
        raise Exception(f"Ошибка при подключении к серверу ЛЭРС с токеном: {e}")

def create_config():
    config = {}

    # Get OwenCloud credentials and authenticate
    while True:
        config['login'] = input("Введите логин для OwenCloud: ")
        config['password'] = input("Введите пароль для OwenCloud: ")

        # Проверка аутентификации
        try:
            token, name, surname, company_name = authenticate(config['login'], config['password'])
            print(f"{Colors.GREEN}Успешная аутентификация в OwenCloud.{Colors.RESET}")
            print(f"{Colors.GREEN}Имя: {name}, Фамилия: {surname}, Компания: {company_name}{Colors.RESET}")
            break  # Выход из цикла при успешной аутентификации
        except Exception as e:
            print(f"{Colors.RED}Ошибка аутентификации: {e}. Попробуйте снова.{Colors.RESET}")

    # Get LERS server details and test connection
    while True:
        config['lers_server_url'] = input("Введите URL сервера ЛЭРС: ")

        # Проверка подключения к серверу ЛЭРС
        try:
            if test_lers_connection(config['lers_server_url']):
                break  # Выход из цикла при успешном подключении
        except Exception as e:
            print(f"{Colors.RED}Ошибка подключения к серверу ЛЭРС: {e}. Попробуйте снова.{Colors.RESET}")

    # Get LERS token and test connection with token
    while True:
        config['lers_token'] = input("Введите токен для ЛЭРС: ")

        # Проверка подключения к серверу ЛЭРС с токеном
        try:
            if test_lers_token(config['lers_server_url'], config['lers_token']):
                break  # Выход из цикла при успешном подключении с токеном
        except Exception as e:
            print(f"{Colors.RED}Ошибка подключения к серверу ЛЭРС с токеном: {e}. Попробуйте снова.{Colors.RESET}")

    # Get send interval
    config['send_interval'] = int(input("Введите интервал отправки данных (в секундах): "))

    # Get parameters configuration
    parameters_config = []
    while True:
        lers_measurepoint_id = input("Введите ID точки учёта ЛЭРС (или нажмите Enter для завершения): ")
        if not lers_measurepoint_id:
            break

        parameters = []
        while True:
            owen_parameter_id = input("Введите ID параметра Owen (или нажмите Enter для завершения): ")
            if not owen_parameter_id:
                break
            lers_data_parameter = input("Введите параметр данных ЛЭРС: ")
            parameters.append({
                "owen_parameter_id": owen_parameter_id,
                "lers_dataParameter": lers_data_parameter
            })

        parameters_config.append({
            "lers_measurepoint_id": lers_measurepoint_id,
            "parameters": parameters
        })

    config['parameters'] = parameters_config

    # Save to config.json
    with open('config.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=4)

    print(f"{Colors.GREEN}Конфигурация успешно сохранена в config.json{Colors.RESET}")

if __name__ == "__main__":
    create_config()