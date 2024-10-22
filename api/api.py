from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os

version = 1

# Crear la app de FastAPI
app = FastAPI()

# Configuración de la conexión a PostgreSQL
DATABASE_URL = "postgresql://postgres:123456@127.0.0.1:5432/postgres"

# Crear el engine de SQLAlchemy
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependencia para crear una sesión de base de datos
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Endpoint para consultar la tabla gold.hired_employees
@app.get("/hired_employees/")
def get_hired_employees():
    try:
        # Conexión directa usando SQLAlchemy
        with engine.connect() as connection:
            # Ejecutar consulta SQL
            result = connection.execute(text("SELECT * FROM gold.hired_employees")).fetchall()

            # Convertir el resultado en un DataFrame (opcional)
            df = pd.DataFrame(result)

            # Convertir DataFrame a lista de diccionarios
            hired_employees = df.to_dict(orient="records")

            return {"data": hired_employees}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocurrió un error: {str(e)}")

#Endpoint para consulta la cantidad de contrataciones po Q por deparmento y job
@app.get('/total_q_deparment_job')
def get_total_hired_by_q():
    try:
        # Conexión directa usando SQLAlchemy
        with engine.connect() as connection:
            # Ejecutar consulta SQL
            result = connection.execute(text("""SELECT department,
                                                job,
                                                COUNT(CASE WHEN quarter = 1 THEN 1 END) AS Q1,
                                                COUNT(CASE WHEN quarter = 2 THEN 1 END) AS Q2,
                                                COUNT(CASE WHEN quarter = 3 THEN 1 END) AS Q3,
                                                COUNT(CASE WHEN quarter = 4 THEN 1 END) AS Q4
                                            FROM gold.hired_employees
                                            WHERE year = 2021
                                            GROUP BY department, job
                                            ORDER BY department ASC, job ASC;
                                            """)).fetchall()

            # Convertir el resultado en un DataFrame (opcional)
            df = pd.DataFrame(result)

            return {"data": df.to_dict(orient="records")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocurrió un error: {str(e)}")
    

@app.get('/most_hired_dept')
def get_most_hired_dept():
    try:
        # Conexión directa usando SQLAlchemy
        with engine.connect() as connection:
            # Ejecutar consulta SQL
            result = connection.execute(text("""with hired_by_dept as (
                                                select 	department_id,
                                                        department,
                                                        count(*) as hired_by_dept
                                                from 	gold.hired_employees a
                                                where 	a.year = 2021
                                                group by 1,2
                                            ),
                                            mean_hired as (
                                                select  avg(hired_by_dept) as avg_hired
                                                from 	hired_by_dept
                                            ),
                                            actual_hired as(
                                                select 	department_id,
                                                        department,
                                                        count(*) as total
                                                from 	gold.hired_employees
                                                group by 1,2
                                            ),
                                            most_hired as (
                                                select 	*
                                                from 	actual_hired a
                                                where	a.total > (select avg_hired from mean_hired)
                                            )

                                            select *
                                            from 	most_hired
                                            order by total desc
                                            """)).fetchall()

            # Convertir el resultado en un DataFrame (opcional)
            df = pd.DataFrame(result)

            return {"data": df.to_dict(orient="records")}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocurrió un error: {str(e)}")


# Para iniciar el servidor: uvicorn api:app --reload
