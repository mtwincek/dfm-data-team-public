import pandas
import sqlalchemy
import psycopg2
import subprocess


class ParsedData(object):
    panda_df = None

    @staticmethod
    def load_data(file_path):
        ParsedData.panda_df = pandas.read_csv(filepath_or_buffer=file_path, delimiter=',', low_memory=False)

    @staticmethod
    def rename_headers():
        for column_name in list(ParsedData.panda_df):
            ParsedData.panda_df = ParsedData.panda_df.rename(columns={column_name: ParsedData.get_header_to_column_dict()[column_name]})

    @staticmethod
    def clean_data():
        ParsedData.panda_df['labels_ids'] = ParsedData.panda_df['labels_ids'].str.replace('--', '')
        ParsedData.panda_df['labels'] = ParsedData.panda_df['labels'].str.replace('--', '')
        ParsedData.panda_df['conversion_rate'] = ParsedData.panda_df['conversion_rate'].str.replace('%', '')
        ParsedData.panda_df['conversion_rate'] = ParsedData.panda_df['conversion_rate'].str.replace(',', '')
        ParsedData.panda_df['ctr'] = ParsedData.panda_df['ctr'].str.replace('%', '')
        ParsedData.panda_df['search_lost_is_rank'] = ParsedData.panda_df['search_lost_is_rank'].str.replace('%', '')
        ParsedData.panda_df['interaction_rate'] = ParsedData.panda_df['interaction_rate'].str.replace('%', '')
        ParsedData.panda_df['day'] = ParsedData.panda_df['day'].str.replace('-', '/')
        ParsedData.panda_df['start_date'] = ParsedData.panda_df['start_date'].str.replace('-', '/')
        ParsedData.panda_df['end_date'] = ParsedData.panda_df['end_date'].str.replace('-', '/')

    @staticmethod
    def save_to_gzip(file_path):
        ParsedData.panda_df.to_csv(path_or_buf=file_path, compression="gzip", index=False)

    @staticmethod
    def create_table(db_name, user_name, path_to_ddl):
        conn = psycopg2.connect("dbname=%s user=%s" % (db_name, user_name))
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS example_report;")
        conn.commit()
        cur.execute(open(path_to_ddl, "r").read())
        conn.commit()
        conn.close()

    @staticmethod
    def insert_into_db(db_name, user_name):
        #################################################################
        # One of the first things I did for this assignment is look at
        # the data and create and appropriate .dll. I realize that I
        # could have used SQLAlchemy above but I did not find a great
        # way to load a .ddl using SQLAlchemy.
        #################################################################
        engine = sqlalchemy.create_engine('postgresql://%s@localhost/%s' % (user_name, db_name))
        ParsedData.panda_df.to_sql('example_report', engine, if_exists="append", index=False)

    @staticmethod
    def db_dump(db_name, file_path_of_dump):
        #################################################################
        # I do not like this solution but I did not have a strong enough
        # grasp of SQLAlchemy to use it for a dump
        #################################################################
        command = "pg_dump -d %s > %s" % (db_name, file_path_of_dump)
        subprocess.Popen(command, shell=True)

    @staticmethod
    def get_header_to_column_dict():
        return {"Day": "day",
                "Customer ID": "customer_id",
                "Campaign ID": "campaign_id",
                "Campaign": "campaign",
                "Campaign state": "campaign_state",
                "Campaign serving status": "campaign_serving_status",
                "Clicks": "clicks",
                "Start date": "start_date",
                "End date": "end_date",
                "Budget": "budget",
                "Budget ID": "budget_id",
                "Budget explicitly shared": "budget_explicitly_shared",
                "Label IDs": "labels_ids",
                "Labels": "labels",
                "Invalid clicks": "invalid_clicks",
                "Conversions": "conversions",
                "Conv. rate": "conversion_rate",
                "CTR": "ctr",
                "Cost": "cost",
                "Impressions": "impressions",
                "Search Lost IS (rank)": "search_lost_is_rank",
                "Avg. position": "average_position",
                "Interaction Rate": "interaction_rate",
                "Interactions": "interactions"}

    @staticmethod
    def get_data_as_html():
        return ParsedData.panda_df.to_html()
