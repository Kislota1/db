import psycopg2
from configparser import ConfigParser
import requests


def config(filename='database.ini', section='postgresql'):
    """Получаем параметры подключения из конфигурационного файла."""
    parser = ConfigParser()
    parser.read(filename)

    db_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_config[param[0]] = param[1]
    else:
        raise Exception(f"Секция {section} не найдена в файле {filename}")

    return db_config


def create_connection():
    """Создаем подключение к базе данных PostgreSQL."""
    connection = None
    try:
        params = config()
        connection = psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка подключения к базе данных: {error}")

    return connection


def create_tables():
    """Создаем таблицы employers и vacancies в базе данных."""
    connection = create_connection()
    cursor = connection.cursor()

    create_employers_table = """
    CREATE TABLE IF NOT EXISTS employers (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        url VARCHAR(255)
    );
    """

    create_vacancies_table = """
    CREATE TABLE IF NOT EXISTS vacancies (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        salary_from INT,
        salary_to INT,
        currency VARCHAR(10),
        url VARCHAR(255),
        employer_id INT REFERENCES employers(id)
    );
    """

    cursor.execute(create_employers_table)
    cursor.execute(create_vacancies_table)
    connection.commit()
    cursor.close()
    connection.close()


def insert_employers(employers_data):
    """Вставляем данные о работодателях в таблицу employers."""
    connection = create_connection()
    cursor = connection.cursor()

    insert_employer_query = """
    INSERT INTO employers (id, name, url)
    VALUES (%s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    for employer in employers_data:
        employer_id = employer['id']
        employer_name = employer['name']
        employer_url = employer['alternate_url']
        cursor.execute(insert_employer_query, (employer_id, employer_name, employer_url))

    connection.commit()
    cursor.close()
    connection.close()


def insert_vacancies(vacancies_data):
    """Вставляем данные о вакансиях в таблицу vacancies."""
    connection = create_connection()
    cursor = connection.cursor()

    insert_vacancy_query = """
    INSERT INTO vacancies (id, name, salary_from, salary_to, currency, url, employer_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    for vacancy in vacancies_data:
        vacancy_id = vacancy['id']
        vacancy_name = vacancy['name']
        salary_from = vacancy['salary']['from'] if vacancy['salary'] else None
        salary_to = vacancy['salary']['to'] if vacancy['salary'] else None
        currency = vacancy['salary']['currency'] if vacancy['salary'] else None
        vacancy_url = vacancy['alternate_url']
        employer_id = vacancy['employer']['id']
        cursor.execute(insert_vacancy_query,
                       (vacancy_id, vacancy_name, salary_from, salary_to, currency, vacancy_url, employer_id))

    connection.commit()
    cursor.close()
    connection.close()


def get_vacancies_for_company(company_id):
    """Получаем данные о вакансиях для компании по её ID."""
    url = "https://api.hh.ru/vacancies"
    params = {
        'employer_id': company_id,
        'per_page': 10
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Ошибка при получении данных для компании {company_id}, статус: {response.status_code}")
        return None


def get_all_vacancies(company_ids):
    """Получаем данные о вакансиях для всех компаний из списка company_ids."""
    vacancies = []
    for company_id in company_ids:
        company_vacancies = get_vacancies_for_company(company_id)
        if company_vacancies:
            vacancies.extend(company_vacancies['items'])
    return vacancies


company_ids = [1455, 2329, 1740, 78638, 741581, 39729, 49728, 78607, 1428, 949463]


def main():
    create_tables()

    vacancies_data = get_all_vacancies(company_ids)

    employers_data = []
    for vacancy in vacancies_data:
        employer = vacancy['employer']
        if employer not in employers_data:
            employers_data.append(employer)

    insert_employers(employers_data)
    insert_vacancies(vacancies_data)


if __name__ == "__main__":
    main()
