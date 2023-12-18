# Оценка производительности базы данных с использованием PostgreSQL, SQLite, Pandas и DuckDB

Этот проект по оценке производительности базы данных анализирует эффективность различных технологий баз данных при выполнении запросов и анализе данных о такси Нью-Йорка (NYC).

## Требования
- Python 3
- Python библиотеки: duckdb, pandas, psycopg2, sqlite3
- Установленная база данных PostgreSQL

## Подготовка
Вы можете загрузить файлы nyc_yellow_tiny.csv и nyc_yellow_big.csv или использовать свои аналогичные тестовые данные. Поместите файлы в ту же директорию, что и ваши скрипты Python.
Ссылки на скачивание:
-nyc_yellow_tiny.csv - https://drive.google.com/file/d/1XWCk4XmgdNUZ8E42ktjGpeeKZeTO9YnJ/view?usp=drive_link
-nyc_yellow_big.csv - https://drive.google.com/file/d/1BlGqraARshWU1FRSZtjTcISfRP3e8Usx/view?usp=drive_link

## Запуск
1. Для начала откройте файл config.py, в нем необходимо ввести данные от своей базе данных PostgreSQL, чтобы подключиться к ней. Вы можете изменить имя переменной file и указать свой. 
В переменной command вы можете ввести любую из данных команд:
-"import" - команда создает базу данных и импортирует данные из csv файла в таблицу (необходимо перед запуском команды "psycopg2")
-"psycopg2" - запускает бенчмарк для Postgress
-"sqlite" - запускает бенчмарк для SQLite
-"pandas" - запускает бенчмарк для Pandas
-"duckdb" - запускает бенчмарк для DuckDB
2. Запустите main.py, который выполнит указанную вами в config.py команду.
