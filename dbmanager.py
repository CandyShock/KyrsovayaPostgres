import psycopg2


class DBManager:
    def __init__(self):
        self.conn = psycopg2.connect(host='localhost', database='Kursovaya', user='postgres', password='fallout4')

    def create_tables(self):
        """Создание таблиц в Postgers"""
        cur = self.conn.cursor()
        cur.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT NOT NULL,
                description TEXT,
                url TEXT NOT NULL
                );
            """)
        cur.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                company_id INT REFERENCES companies(id),
                salary_min INT,
                salary_max INT,
                url TEXT NOT NULL
                );
            """)
        self.conn.commit()

    def drop_tables(self):
        """Фунция для удаления таблиц"""
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS vacancies;")
        cur.execute("DROP TABLE IF EXISTS companies;")
        self.conn.commit()

    def data_insert(self, company, vacancies):
        """Заполнение таблиц данными"""
        cur = self.conn.cursor()
        cur.execute(f"""
        INSERT INTO companies (id, name, city, description, url) 
        VALUES (%s, %s, %s, %s, %s)""",
                    (int(company['id']), company['name'], company['area']['name'],
                     company['description'], company['alternate_url'])
                    )
        self.conn.commit()
        for item in vacancies['items']:
            from_ = to_ = None
            if item['salary']:
                from_ = item['salary']['from']
                to_ = item['salary']['to']
            cur.execute(f"""
                INSERT INTO vacancies (name, company_id, salary_min, salary_max, url) 
                VALUES (%s, %s, %s, %s, %s)""", (
                item['name'], item['employer']['id'], from_, to_, item['alternate_url']
            )
                        )
            self.conn.commit()

    def get_companies_and_vacancies_count(self):
        """Возвращает компании и количество связанных вакансий"""
        cur = self.conn.cursor()
        cur.execute("""
        SELECT companies.name, COUNT(vacancies.id)
        FROM vacancies JOIN companies ON vacancies.company_id = companies.id
        GROUP BY companies.name
        """)
        return cur.fetchall()

    def get_all_vacancies(self):
        """Возвращает все вакансии"""
        cur = self.conn.cursor()
        cur.execute("""
                SELECT companies.name, vacancies.name, vacancies.salary_min, vacancies.salary_max, vacancies.url
                FROM vacancies JOIN companies ON vacancies.company_id = companies.id;
                """)
        return cur.fetchall()

    def get_avg_salary(self):
        """Возвращает среднюю зп"""
        cur = self.conn.cursor()
        cur.execute("""
                        SELECT AVG(vacancies.salary_max)
                        FROM vacancies;
                        """)
        return cur.fetchone()

    def get_vacancies_with_higher_salary(self):
        """Возвращают вакансию где максимальная зп больше средней"""
        cur = self.conn.cursor()
        cur.execute(f"""
                       SELECT companies.name, vacancies.name, vacancies.salary_min, vacancies.salary_max, vacancies.url
                       FROM vacancies JOIN companies ON vacancies.company_id = companies.id
                       WHERE vacancies.salary_max > ({self.get_avg_salary()[0]});
                       """)
        return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """Возвращает вакансию по ключевому слову"""
        cur = self.conn.cursor()
        cur.execute(f"""
                       SELECT * FROM vacancies 
                       WHERE vacancies.name LIKE '%{keyword}%';
                       """)
        return cur.fetchall()




