--version = 1

-- Crear los esquemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Crear la tabla hired_employees en el esquema bronze
CREATE TABLE IF NOT EXISTS bronze.hired_employees (
    id INTEGER,
    name VARCHAR(255),
    datetime TIMESTAMP,
    department_id INTEGER,
    job_id INTEGER
);

-- Crear la tabla departments en el esquema bronze
CREATE TABLE IF NOT EXISTS bronze.departments (
    id INTEGER,
    department VARCHAR(255)
);

-- Crear la tabla jobs en el esquema bronze
CREATE TABLE IF NOT EXISTS bronze.jobs (
    id INTEGER,
    job VARCHAR(255)
);


-- Crear la tabla hired_employees en el esquema silver
CREATE TABLE IF NOT EXISTS silver.hired_employees (
    id INTEGER,
    name VARCHAR(255),
    datetime TIMESTAMP,
    department_id INTEGER,
    job_id INTEGER
);

-- Crear la tabla departments en el esquema bronze
CREATE TABLE IF NOT EXISTS silver.departments (
    id INTEGER,
    department VARCHAR(255)
);

-- Crear la tabla jobs en el esquema bronze
CREATE TABLE IF NOT EXISTS silver.jobs (
    id INTEGER,
    job VARCHAR(255)
);

