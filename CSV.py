import csv
from datetime import datetime

def categorize_blood_pressure(bp_value):
    """Catégoriser la tension artérielle selon les critères AHA."""
    try:
        systolic, diastolic = map(int, bp_value.split("/"))
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
    with open(old_csv_file_name, "r", encoding="utf-8", newline="") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        data = list(reader)

    # indices utiles
    idx_gender = header.index("Gender")
    idx_reg_date = header.index("User_Registration_Date")
    idx_reg_time = header.index("User_Registration_Time")
    idx_checkup_date = header.index("Last_Checkup_Date")
    idx_checkup_time = header.index("Last_Checkup_Time")
    idx_no_of_checkups = header.index("No_of_Checkups")
    idx_no_of_missed_checkup = header.index("No_of_Missed_Checkups")
    idx_bp = header.index("Blood_Pressure")

    # colonnes à supprimer
    drop_cols = {"Reminder_Date", "Gender", "No_of_Checkups", "No_of_Missed_Checkups"}
    keep_indices = [i for i, col in enumerate(header) if col not in drop_cols]

    # nouveau header
    new_header = [header[i] for i in keep_indices]
    new_header.append("Checkup")
    new_header.append("BP_Category")

    new_data = []
    for row in data:
        # corriger dates pour les femmes
        reg_date, chk_date = row[idx_reg_date], row[idx_checkup_date]

        try:
            reg_dt = datetime.fromisoformat(reg_date)
            chk_dt = datetime.fromisoformat(chk_date)
        except:
            reg_dt, chk_dt = reg_date, chk_date

        try:
            if reg_dt > chk_dt:
                row[idx_reg_date], row[idx_checkup_date] = row[idx_checkup_date], row[idx_reg_date]
                row[idx_reg_time], row[idx_checkup_time] = row[idx_checkup_time], row[idx_reg_time]
        except:
            if reg_date > chk_date:
                row[idx_reg_date], row[idx_checkup_date] = row[idx_checkup_date], row[idx_reg_date]
                row[idx_reg_time], row[idx_checkup_time] = row[idx_checkup_time], row[idx_reg_time]

        # calculer Checkup
        try:
            checkup_done = int(row[idx_no_of_checkups]) - int(row[idx_no_of_missed_checkup])
        except:
            checkup_done = ""

        # catégoriser BP
        bp_value = row[idx_bp]
        bp_category = categorize_blood_pressure(bp_value)

        # construire ligne finale
        final_row = [row[i] for i in keep_indices]
        final_row.append(str(checkup_done))
        final_row.append(bp_category)
        new_data.append(final_row)

    # écrire le nouveau CSV
    with open(new_csv_file_name, "w", encoding="utf-8", newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(new_header)
        writer.writerows(new_data)
