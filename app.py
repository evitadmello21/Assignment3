from flask import Flask, request, jsonify, render_template, url_for
import csv
import os
import requests
import threading
import queue
import json
import logging

app = Flask(__name__)

def load_config():
    with open('config.json') as config_file:
        return json.load(config_file)

config = load_config()

csv_file = config.get("csv_file", "student_data.csv")
csv_header = ["RollNo", "Name", "English", "Maths", "Science"]

log_path = config.get("log_path", "app.log")
log_level = logging.DEBUG  # Set to desired logging level

logger = logging.getLogger()
logger.setLevel(log_level)

file_handler = logging.FileHandler(log_path)
file_handler.setLevel(log_level)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info("Logging setup complete.")

def read_csv():
    """
    Read data from the CSV file.
    :return: List of dictionaries containing CSV data.
    """
    data = []
    try:
        if os.path.isfile(csv_file):
            with open(csv_file, mode="r", newline="") as file:
                reader = csv.DictReader(file)
                data = list(reader)
            logging.info("CSV file read successfully.")
    except Exception as e:
        logging.error(f"Error in reading CSV file: {e}")

    return data

def write_csv(row):
    """
    Write data to CSV file.
    :param row: List of dictionaries to write to the CSV file.
    :return: True if write was successful, False otherwise.
    """
    try:
        with open(csv_file, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=csv_header)
            writer.writeheader()
            writer.writerows(row)
        logging.info("CSV file written successfully.")
        return True
    except Exception as e:
        logging.error(f"Error in writing CSV file: {e}")
        return False

@app.route("/", methods=["GET"])
def index():
    """
    Render the home page.
    """
    return render_template('index.html')

@app.route("/insert", methods=["GET"])
def insert_page():
    """
    Render the insert data page.
    """
    return render_template('insert.html')

@app.route("/insert", methods=["POST"])
def insert_data():
    """
    Insert a new record into the CSV file.
    :return: JSON response indicating the success or failure of the operation.
    """
    data = request.form.to_dict()

    if not all(field in data for field in csv_header):
        logging.warning("Insert request missing required fields.")
        return jsonify({'error': 'Missing required fields'}), 400

    row = read_csv()

    if any(item['RollNo'] == data['RollNo'] for item in row):
        logging.warning(f"Record with RollNo {data['RollNo']} already exists.")
        return jsonify({'error': f"Record with the RollNo {data['RollNo']} already exists"}), 400

    row.append(data)
    if write_csv(row):
        logging.info(f"Record inserted: {data}")
        return jsonify({"message": "Record inserted successfully"}), 201
    else:
        logging.error("Failed to write record.")
        return jsonify({"error": "Failed to write record"}), 500

@app.route("/remove", methods=["GET", "POST"])
def remove_form():
    """
    Render the remove data page or process the removal of a record.
    """
    if request.method == "POST":
        rollno = request.form.get("RollNo")

        if rollno:
            try:
                response = requests.delete(url_for("remove_data", rollno=rollno, _external=True))
                logging.info(f"Remove request for RollNo: {rollno}")
                return response.json()
            except Exception as e:
                logging.error(f"Error in remove request: {e}")
                return jsonify({"error": "Failed to process request"}), 500

    return render_template("remove.html")

@app.route("/remove/<string:rollno>", methods=["DELETE"])
def remove_data(rollno):
    """
    Remove a record from the CSV file.
    :param rollno: Roll number of the record to remove.
    :return: JSON response indicating the success or failure of the operation.
    """
    records = read_csv()
    filtered_records = [row for row in records if row["RollNo"] != rollno]

    if len(records) == len(filtered_records):
        logging.warning(f"Record with RollNo {rollno} not found.")
        return jsonify({"error": "Record not found"}), 404

    if write_csv(filtered_records):
        logging.info(f"Record removed: RollNo {rollno}")
        return jsonify({"message": "Record deleted successfully"}), 200
    else:
        logging.error("Failed to delete record.")
        return jsonify({"error": "Failed to delete record"}), 500

@app.route("/update", methods=["GET", "POST"])
def update_form():
    """
    Render the update data page or process the update of a record.
    """
    if request.method == "POST":
        data = request.form.to_dict()

        try:
            response = requests.put(url_for("update_data", _external=True), json=data)
            logging.info(f"Update request for RollNo: {data.get('RollNo')}")
            return response.json()
        except Exception as e:
            logging.error(f"Error in update request: {e}")
            return jsonify({"error": "Failed to process request"}), 500

    return render_template("update.html")

@app.route("/update", methods=["PUT"])
def update_data():
    """
    Update a record in the CSV file.
    :return: JSON response indicating the success or failure of the operation.
    """
    data = request.json
    records = read_csv()
    updated = False

    for row in records:
        if row["RollNo"] == data["RollNo"]:
            row.update(data)
            updated = True

    if not updated:
        logging.warning(f"Update request: Record with RollNo {data['RollNo']} not found.")
        return jsonify({"error": "Record not found"}), 404

    if write_csv(records):
        logging.info(f"Record updated: {data}")
        return jsonify({"message": "Record updated successfully"}), 200
    else:
        logging.error("Failed to update record.")
        return jsonify({"error": "Failed to update record"}), 500

@app.route("/read", methods=["GET"])
def read_data():
    """
    Read a record from the CSV file by RollNo.
    :return: JSON response containing the record or an error message.
    """
    rollno = request.args.get("RollNo")

    if rollno:
        records = read_csv()

        for row in records:
            if row["RollNo"] == rollno:
                logging.info(f"Record read: {row}")
                return jsonify(row), 200

        logging.warning(f"Read request: Record with RollNo {rollno} not found.")
        return jsonify({"error": "Record not found"}), 404

    return render_template("read.html")

def calculate_avg(records, result_queue):
    """
    Calculate the average of marks for a list of student records.
    :param records: List of student records.
    :param result_queue: Queue to put the calculated averages into.
    """
    student_avg = []
    for row in records:
        try:
            english = float(row["English"])
            maths = float(row["Maths"])
            science = float(row["Science"])

            avg = (english + maths + science) / 3
            avg = round(avg, 2)
        except Exception as e:
            logging.error(f"Error in calculating average: {e}")
            avg = 0

        student_avg.append({
            "RollNo": row["RollNo"],
            "Name": row["Name"],
            "Average": avg
        })

    result_queue.put(student_avg)

@app.route("/average", methods=["GET"])
def average():
    """
    Calculate and return the average marks for all students.
    :return: JSON response containing the averages of all students.
    """
    records = read_csv()
    num_records = len(records)
    record_per_thread = config.get("thread_size", 10)

    num_thread = num_records // record_per_thread + 1
    threads = []
    result_queue = queue.Queue()

    for i in range(num_thread):
        start = i * record_per_thread
        end = min(start + record_per_thread, num_records)
        record = records[start:end]

        thread = threading.Thread(target=calculate_avg, args=(record, result_queue))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    all_student_avg = []
    while not result_queue.empty():
        all_student_avg.extend(result_queue.get())

        logging.info("Calculated average marks for all students.")
        return jsonify(all_student_avg), 200

if __name__ == '__main__':
    # Create the CSV file with headers if it doesn't exist
    if not os.path.isfile(csv_file):
        with open(csv_file, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(csv_header)
        logging.info(f"CSV file created: {csv_file}")

    app.run(debug=True)

