from flask import Flask, redirect, url_for
from ParsedData import ParsedData
app = Flask(__name__)


@app.route("/")
def output():
    #################################################################
    # Normally I would not hard-code the filepath but I just wanted
    # the script to run without any configuration on the user's end.
    #################################################################
    # Load data into dataframe
    ParsedData.load_data(file_path="example_report.csv")

    # Rename headers to column names
    ParsedData.rename_headers()

    # Clean/Normalize data
    ParsedData.clean_data()

    # Save parsed results to gzip file
    ParsedData.save_to_gzip(file_path="parsed_results.csv.gz")

    # Create DB table
    #################################################################
    # Per the directions for this exercise I am assuming the
    # database/user were setup before running this program
    #################################################################
    ParsedData.create_table(db_name="mwincek", user_name="mwincek", path_to_ddl="create_db.ddl")

    # INSERT data into DB table
    ParsedData.insert_into_db(db_name="mwincek", user_name="mwincek")

    # Dump data from DB into file
    ParsedData.db_dump(db_name="mwincek", file_path_of_dump="db_dump.sql")

    return redirect(url_for("results"))


@app.route("/results")
def results():
    return ParsedData.get_data_as_html()


if __name__ == "__main__":
    app.run(debug=True)
