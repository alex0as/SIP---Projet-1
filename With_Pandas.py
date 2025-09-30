import sqlite3
import utils
import csv
from datetime import datetime


def get_db_connexion():
    # Loads the app config into the dictionary app_config.
    app_config = utils.load_config()

    if not app_config:
        print("Error: while loading the app configuration")
        return None

    # From the configuration, gets the path to the database file.
    db_file = app_config["db"]

    # Open a connection to the database.
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row

    return conn


def close_db_connexion(cursor, conn):
    """Close a database connexion and the cursor.

    Parameters
    ----------
    cursor
        The object used to query the database.
    conn
        The object used to manage the database connection.
    """
    cursor.close()
    conn.close()

import pandas as pd
from datetime import datetime

def categorize_blood_pressure(bp_value):
    try:
        systolic, diastolic = map(int, str(bp_value).split("/"))
    except:
        return "Unknown"

    if systolic > 180 or diastolic > 120:
        return "Hypertensive Crisis"
    elif systolic >= 140 or diastolic >= 90:
        return "Hypertension Stage 2"
    elif systolic >= 130 or diastolic >= 80:
        return "Hypertension Stage 1"
    elif systolic >= 120 and diastolic < 80:
        return "Elevated"
    elif systolic < 120 and diastolic < 80:
        return "Normal"
    else:
        return "Unknown"

def transform_data(old_csv_file_name, new_csv_file_name):
    # Lire le CSV
    df = pd.read_csv(old_csv_file_name)

    # Calculer la colonne Checkup
    df["Checkup"] = df["No_of_Checkups"] - df["No_of_Missed_Checkups"]

    # Supprimer les colonnes inutiles
    df = df.drop(columns=["Reminder_Date", "Gender", "No_of_Checkups", "No_of_Missed_Checkups"])

    # Corriger les dates si User_Registration_Date > Last_Checkup_Date
    wrong_dates_mask = df["User_Registration_Date"] > df["Last_Checkup_Date"]
    df.loc[wrong_dates_mask, ["User_Registration_Date", "Last_Checkup_Date"]] = df.loc[wrong_dates_mask, ["Last_Checkup_Date", "User_Registration_Date"]].values
    df.loc[wrong_dates_mask, ["User_Registration_Time", "Last_Checkup_Time"]] = df.loc[wrong_dates_mask, ["Last_Checkup_Time", "User_Registration_Time"]].values

    # Ajouter la colonne BP_Category
    df["BP_Category"] = df["Blood_Pressure"].apply(categorize_blood_pressure)

    # Sauvegarder le CSV
    df.to_csv(new_csv_file_name, index=False)

transform_data("C:/Users/Bonin/Desktop/Centrale Supélec/Première année/SIP/tp_pregnancies_squelette/data/pregnancies.csv", "C:/Users/Bonin/Desktop/Centrale Supélec/Première année/SIP/tp_pregnancies_squelette/data/new_pregnancies.csv")

def create_database(cursor, conn):
    """Creates the Pregnancies 2023 database

    Parameters
    ----------
    cursor
        The object used to query the database.
    conn
        The object used to manage the database connection.

    Returns
    -------
    bool
        True if the database could be created, False otherwise.
    """

    # We open a transaction.
    # A transaction is a sequence of read/write statements that
    # have a permanent result in the database only if they all succeed.
    #
    # More concretely, in this function we create many tables in the database.
    # The transaction is therefore a sequence of CREATE TABLE statements such as :
    #
    # BEGIN
    # CREATE TABLE XXX
    # CREATE TABLE YYY
    # CREATE TABLE ZZZ
    # ....
    #
    # If no error occurs, all the tables are permanently created in the database.
    # If an error occurs while creating a table (for instance YYY), no table will be created, even those for which
    # the statement CREATE TABLE has already been executed (in this example, XXX).
    #
    # When we start a transaction with the statement BEGIN, we must end it with either COMMIT
    # or ROLLBACK.
    #
    # * COMMIT is called when no error occurs. After calling COMMIT, the result of all the statements in
    # the transaction is permanetly written to the database. In our example, COMMIT results in actually creating all the tables
    # (XXX, YYY, ZZZ, ....)
    #
    # * ROLLBACK is called when any error occurs in the transaction. Calling ROLLBACK means that
    # the database is not modified (in our example, no table is created).
    #
    #
    cursor.execute("BEGIN")

    # Create the tables.
    tables = {
        "Analyst": """
            CREATE TABLE IF NOT EXISTS Analyst(
                id INTEGER PRIMARY KEY,
                username TEXT UNIQUE,
                password BLOB
            );
        """,

        "Hospital": """
            CREATE TABLE IF NOT EXISTS Hospital(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT
            );
        """,

        "Woman": """
            CREATE TABLE IF NOT EXISTS Woman(
                id INTEGER PRIMARY KEY,
                name TEXT,
                birth_date TEXT,
                blood_type TEXT,
                hospital_id INTEGER,
                FOREIGN KEY(hospital_id) REFERENCES Hospital(id)
            );
        """,

        "Pregnancy": """
            CREATE TABLE IF NOT EXISTS Pregnancy(
                id INTEGER PRIMARY KEY,
                woman_id INTEGER,
                analyst_id INTEGER,
                first_registration_date TEXT,
                delivery_date TEXT,
                baby_gender TEXT,
                delivery_type TEXT,
                number_of_checkups INTEGER,
                number_of_missed_checkups INTEGER,
                FOREIGN KEY(woman_id) REFERENCES Woman(id),
                FOREIGN KEY(analyst_id) REFERENCES Analyst(id)
            );
        """,

        "Checkup": """
            CREATE TABLE IF NOT EXISTS Checkup(
                id INTEGER PRIMARY KEY,
                pregnancy_id INTEGER,
                date TEXT,
                time TEXT,
                weight REAL,
                blood_pressure TEXT,
                gestational_age INTEGER,
                fetal_heart_rate INTEGER,
                anomaly_presence INTEGER,
                maternal_mental_health INTEGER,
                FOREIGN KEY(pregnancy_id) REFERENCES Pregnancy(id)
            );
        """
    }

    try:
        # To create the tables, we call the function cursor.execute() and we pass it the
        # CREATE TABLE statement as a parameter.
        # The function cursor.execute() can raise an exception sqlite3.Error.
        # That's why we write the code for creating the tables in a try...except block.
        for tablename in tables:
            print(f"Creating table {tablename}...", end=" ")
            cursor.execute(tables[tablename])
            print("OK")

    ###################################################################

    # Exception raised when something goes wrong while creating the tables.
    except sqlite3.Error as error:
        print("An error occurred while creating the tables: {}".format(error))
        # IMPORTANT : we rollback the transaction! No table is created in the database.
        conn.rollback()
        # Return False to indicate that something went wrong.
        return False

    # If we arrive here, that means that no error occurred.
    # IMPORTANT : we must COMMIT the transaction, so that all tables are actually created in the database.
    conn.commit()
    print("Database created successfully")
    # Returns True to indicate that everything went well!
    return True


def populate_database(cursor, conn, csv_file_name):
    """Populate the database with data in a CSV file.

    Parameters
    ----------
    cursor
        The object used to query the database.
    conn
        The object used to manage the database connection.
    csv_file_name
        Name of the CSV file where the data are.

    Returns
    -------
    bool
        True if the database is correctly populated, False otherwise.
    """
    try:
        df = pd.read_csv(csv_file_name)

        # Dictionnaire pour stocker hospital_name -> id
        hospital_ids = {}

        for _, row in df.iterrows():
            # --- Hospital ---
            hospital_name = row["Hospital_Name"]
            if hospital_name not in hospital_ids:
                cursor.execute("INSERT OR IGNORE INTO Hospital(name) VALUES(?)", (hospital_name,))
                hospital_ids[hospital_name] = cursor.lastrowid or cursor.execute("SELECT id FROM Hospital WHERE name=?", (hospital_name,)).fetchone()[0] #on récupère l'ID pour en faire une clé étrangère

            hospital_id = hospital_ids[hospital_name]

            # --- Woman ---
            cursor.execute("""
                INSERT INTO Woman(name, birth_date, blood_type, hospital_id)
                VALUES(?,?,?,?)
            """, (row["Name"], row["Date_of_Birth"], row["Mother_Blood_Type"], hospital_id))
            woman_id = cursor.lastrowid

            # --- Pregnancy ---
            analyst_id = 1  # simplification si un seul analyst
            cursor.execute("""
                INSERT INTO Pregnancy(
                    woman_id, analyst_id, first_registration_date, delivery_date,
                    baby_gender, delivery_type, number_of_checkups
                )
                VALUES(?,?,?,?,?,?,?)
            """, (
                woman_id, analyst_id,
                row["User_Registration_Date"],
                row["Delivery_Date"],
                row["Baby_Gender"],
                row["Delivery_Type"],
                row["Checkup"]
            ))
            pregnancy_id = cursor.lastrowid

            # --- Checkup ---
            cursor.execute("""
                INSERT INTO Checkup(
                    pregnancy_id, date, time, weight, blood_pressure,
                    gestational_age, fetal_heart_rate, anomaly_presence, maternal_mental_health, bp_category
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (
                pregnancy_id,
                row["Last_Checkup_Date"],
                row["Last_Checkup_Time"],
                row["Weight(kg)"],
                row["Blood_Pressure"],
                row["Gestational_Age"],
                row["Fetal_Heart_Rate"],
                int(row["Anomaly"]),
                int(row["Maternal_Mental_Health"]),
                row["BP_Category"]
            ))

        conn.commit()
        return True

    except Exception as e:
        print("Error populating database:", e)
        conn.rollback()
        return False


def init_database():
    """Initialise the database by creating the database
    and populating it.
    """
    try:
        conn = get_db_connexion()

        # The cursor is used to execute queries to the database.
        cursor = conn.cursor()

        # Creates the database. THIS IS THE FUNCTION THAT YOU'LL NEED TO MODIFY
        create_database(cursor, conn)

        # Populates the database.
        # TODO - add call to populate_database()

        # Closes the connection to the database
        close_db_connexion(cursor, conn)
    except Exception as e:
        print("Error: Database cannot be initialised:", e)
