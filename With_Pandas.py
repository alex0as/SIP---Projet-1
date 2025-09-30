
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
