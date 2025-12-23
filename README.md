-- Commands to run .py file

export DB_PASSWORD="Vip@1113@AP"
python3 sync_google_sheet.py 

Steps:

Upload excel  to google sheets
Create table in Supabase
Create ETL script with .py extension. In this add sheet url, Supabase creds.
Create GitHub repo, Github Actions -> Env Variables -> Add Supabase Creds.
Create Automation Actions .yaml file and set cron for suppose 6 hours.
