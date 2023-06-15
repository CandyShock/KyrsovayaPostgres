from pprint import pprint

from utils import get_request
from dbmanager import DBManager


def main():
    db = DBManager()
    db.create_tables()
    """id компаний"""
    employees_id = [1, 6, 8, 13, 14, 15, 19, 26, 35, 36]

    for employer in employees_id:
        company = get_request(f'https://api.hh.ru/employers/{employer}')
        vacancies = get_request(f'https://api.hh.ru/vacancies?employer_id={employer}&per_page=25')
        db.data_insert(company, vacancies)

    input('Нажмите любую кнопку')
    pprint(db.get_all_vacancies())
    input('Нажмите любую кнопку')
    pprint(db.get_companies_and_vacancies_count())
    input('Нажмите любую кнопку')
    pprint(db.get_vacancies_with_higher_salary())
    input('Нажмите любую кнопку')
    keyword = input('Введите слово')
    pprint(db.get_vacancies_with_keyword(keyword))
    input('Нажмите любую кнопку')
    pprint(db.get_avg_salary())


if __name__ == '__main__':
    main()
