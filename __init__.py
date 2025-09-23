import sqlite3
import utils


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


def transform_csv(old_csv_file_name, new_csv_file_name):
    """Write a new CSV file based on the input CSV file by adding
    new columns to obtain a CSV file that is easier to read.

    Parameters
    ----------
    csv_file_name
        Name of the CSV file to transform
    new_csv_file_name
        Name of the new CSV file
    """
    with open(old_csv_file_name, "r", encoding="utf-8") as infile:
        lines = [line.strip() for line in infile.readlines()]

    header = lines[0].split(",")
    data = [line.split(",") for line in lines[1:]]

    # Trouver les indices des colonnes utiles
    idx_gender = header.index("Gender")
    idx_reg_date = header.index("User_Registration_Date")
    idx_reg_time = header.index("User_Registration_Time")
    idx_checkup_date = header.index("Last_Checkup_Date")
    idx_checkup_time = header.index("Last_Checkup_Time")

    # Supprimer Reminder Date et Gender de l'entête
    drop_cols = {"Reminder_Date", "Gender"}
    keep_indices = [i for i, col in enumerate(header) if col not in drop_cols]
    new_header = [header[i] for i in keep_indices]

    new_data = []
    for row in data:
        # Vérifier si c'est une femme et si les dates sont inversées
        gender = row[idx_gender].strip().lower()
        if gender == "female":
            if row[idx_reg_date] > row[idx_checkup_date]:
                # swap dates et heures
                row[idx_reg_date], row[idx_checkup_date] = row[idx_checkup_date], row[idx_reg_date]
                row[idx_reg_time], row[idx_checkup_time] = row[idx_checkup_time], row[idx_reg_time]

        # Filtrer les colonnes à garder
        new_row = [row[i] for i in keep_indices]
        new_data.append(new_row)

    # Écrire le nouveau CSV
    with open(new_csv_file_name, "w", encoding="utf-8") as outfile:
        outfile.write(",".join(new_header) + "\n")
        for row in new_data:
            outfile.write(",".join(row) + "\n")

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
                username TEXT PRIMARY KEY,
                password BINARY(256)
            );
            """,
        # TODO: COMPLETE THE CODE HERE TO CREATE THE OTHER TABLES ####
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
    # TODO
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
