import streamlit as st
import pandas as pd
import sqlglot
from sqlglot.dialects import Dialect
from io import BytesIO
import zipfile
import traceback
import os, subprocess
from datetime import datetime
import requests

# Inject custom CSS for button placement
st.markdown("""
    <style>
        .block-container {
            padding-top: 1.5rem !important;
            max-width: 85%;
            margin: auto;
        }
        .button-container {
            display: flex;
            justify-content: space-between; /* Spread buttons evenly */
            gap: 10px;
        }

        .stTextArea textarea {
            height: 200px !important;
            font-size: 14px;
            width: 100%;
        }

        /* Button Styling */
        div.stButton > button {
            background-color: #28a745 !important; /* Green */
            color: white !important;
            border-radius: 8px !important;
            border: none !important;
            padding: 10px 20px !important;
            font-size: 16px !important;
            font-weight: bold !important;
            transition: background-color 0.3s ease !important;
            margin: 0px 2px !important; /* Reduce margin further */
        }

        div.stButton > button:hover {
            background-color: #218838 !important; /* Darker green on hover */
        }

        /* Flex container for buttons */
        .button-container {
            display: flex;
            justify-content: center; /* Center the buttons */
            gap: 2px; /* Reduce space between buttons */
        }
    </style>
""", unsafe_allow_html=True)

def save_sql_files_locally(src_nm, dataset_nm, land_sql_script, stage_sql_script, rds_sql_script):
    try:
        # Define the temp directory path inside the script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        temp_dir = os.path.join(script_dir, "temp")

        # Create temp directory if it doesn't exist
        os.makedirs(temp_dir, exist_ok=True)

        # Define file paths
        sql_files = {
            os.path.join(temp_dir, f"{src_nm}_{dataset_nm}_land.sql"): land_sql_script,
            os.path.join(temp_dir, f"{src_nm}_{dataset_nm}_stage.sql"): stage_sql_script,
            os.path.join(temp_dir, f"{src_nm}_{dataset_nm}_rds.sql"): rds_sql_script,
        }

        # Save each SQL script to the temp directory
        for file_path, content in sql_files.items():
            if content:  # Ensure content is not empty
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)
                print(f"Saved: {file_path}")
            else:
                print(f"Skipped empty file: {file_path}")

        return temp_dir  # Return the directory path for reference

    except Exception as e:
        print(f"Error saving SQL files: {e}")
        return None

def create_downloadable_zip(sheets_data, src_nm, dataset_nm, land_sql_script, stage_sql_script, rds_sql_script):
    output = BytesIO()
    
    with zipfile.ZipFile(output, 'w') as zf:
        # Verify sheets_data before writing Excel
        if sheets_data and isinstance(sheets_data, dict):
            print("Creating Excel file...")  # Debug
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                for sheet_name, df in sheets_data.items():
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        df.to_excel(writer, index=False, sheet_name=sheet_name)
            writer.close()
            excel_buffer.seek(0)
            zf.writestr(f"{src_nm}_{dataset_nm}_onboarding_template.xlsx", excel_buffer.getvalue())
        else:
            print("Error: sheets_data is None or not a dictionary of DataFrames!")

        # Debug: Ensure SQL scripts are non-empty
        if land_sql_script:
            print("Adding Land SQL script to ZIP")
            zf.writestr(f"{src_nm}_{dataset_nm}_land.sql", land_sql_script)
        else:
            print("Error: Land SQL script is empty!")

        if stage_sql_script:
            print("Adding Stage SQL script to ZIP")
            zf.writestr(f"{src_nm}_{dataset_nm}_stage.sql", stage_sql_script)
        else:
            print("Error: Stage SQL script is empty!")

        if rds_sql_script:
            print("Adding RDS SQL script to ZIP")
            zf.writestr(f"{src_nm}_{dataset_nm}_rds.sql", rds_sql_script)
        else:
            print("Error: RDS SQL script is empty!")

    output.seek(0)

    if output.getvalue():  # Check if ZIP is not empty
        print("ZIP file successfully created!")
    else:
        print("Error: ZIP file is empty!")

    return output


def git_push_files_to_feature_branch(files, branch_name, folder):
    try:
        GIT_USERNAME = os.getenv('GIT_USERNAME')
        GIT_TOKEN = os.getenv('GIT_TOKEN')
        GIT_REPO = f'https://{GIT_USERNAME}:{GIT_TOKEN}@github.com/akashgarje/ingestion-onboarding-automation.git'
        REPO_NAME = 'ingestion-onboarding-automation'
        REPO_OWNER = 'akashgarje'
        st.write(GIT_REPO)

        # Get the script's directory (local Git repo)
        repo_path = os.path.dirname(os.path.abspath(__file__))
        os.chdir(repo_path)

        remotes = subprocess.check_output(["git", "remote", "-v"], text=True, cwd=repo_path)
        # st.write(remotes)

        # Ensure the repository is initialized
        if not os.path.exists(os.path.join(repo_path, ".git")):
            subprocess.run(["git", "init"], check=True, cwd=repo_path)
            subprocess.run(["git", "remote", "add", "origin", GIT_REPO], check=True, cwd=repo_path)
            subprocess.run(["git", "pull", "origin", "main"], check=True, cwd=repo_path)

        # Detect the correct branch (main or master)
        branches = subprocess.check_output(["git", "branch", "-a"], text=True, cwd=repo_path)
        main_branch = "main" if "main" in branches else "master"

        subprocess.run(["git", "pull", "origin", main_branch], check=True, cwd=repo_path)

        # Create and switch to a new feature branch
        subprocess.run(["git", "checkout", "-b", branch_name], check=True, cwd=repo_path)
        subprocess.run(["git", "branch", "--set-upstream-to=origin/main", branch_name], check=True, cwd=repo_path)

        # Ensure the folder exists
        folder_path = os.path.join(repo_path, folder)
        os.makedirs(folder_path, exist_ok=True)

        # Move files to the target folder before adding them to Git
        file_paths = []
        for file in files:
            destination = os.path.join(folder_path, os.path.basename(file))
            os.rename(file, destination)  # Move file to folder
            file_paths.append(destination)
        st.write(f"Files in folder {folder_path}: {os.listdir(folder_path)}")

        # Add files to Git
        subprocess.run(["git", "add", folder], check=True, cwd=repo_path)

        # Commit the changes
        subprocess.run(["git", "commit", "-m", f"Added new files to {folder} in feature branch"], check=True, cwd=repo_path)

        current_branch = subprocess.check_output(["git", "branch", "--show-current"], text=True, cwd=repo_path)
        # st.write(f"Current branch: {current_branch}")

        # Push the changes to the remote repository
        subprocess.run(["git", "push", "--set-upstream", "origin", branch_name], check=True, cwd=repo_path)

        # Create a pull request
        pr_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/pulls"
        pr_data = {
            "title": f"Merge {branch_name} to dev",
            "head": branch_name,
            "base": "dev",
            "body": "This PR merges the feature branch to the dev branch."
        }
        response = requests.post(pr_url, json=pr_data, auth=(GIT_USERNAME, GIT_TOKEN))

        if response.status_code == 201:
            return "Git push successful and PR created!"
        else:
            return f"Git push successful but PR creation failed: {response.json()}"

    except subprocess.CalledProcessError as e:
        return f"Git push failed: {e}"

    except Exception as e:
        return f"An error occurred: {e}"

def infer_snowflake_type(col_data):
    """Infer Snowflake-compatible SQL data type from column values."""
    if col_data.empty:
        return "VARCHAR(255)"  # Default for empty columns

    if col_data.str.isnumeric().all():
        return "NUMBER(38,0)"  # Integer (large precision)

    try:
        if pd.to_datetime(col_data, errors='coerce').notna().all():
            return "TIMESTAMP_NTZ"  # Snowflake timestamp
    except:
        pass  # Ignore conversion errors

    return "VARCHAR(255)"  # Default to variable-length string

st.title("Data Onboarding Tool")

# Input fields in one line (4 per row)
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    src_nm = st.selectbox("Source Name",
    [
    "fin_user",
    "smrt_fctry",
    "comrcl_user",
    "keu_bw",
    "eu_nielsen",
    "anaplan",
    "kg_bw_bpc",
    "tpm_optmztn",
    "b_and_m_user",
    "kna_ecc",
    "keu_ecc",
    "keu_scm",
    "sc_user",
    "sif",
    "eu_ktie"
    ])
    fmt_type_cd = st.selectbox("Format Type", ["csv", "excel","txt","zip",'json','parquet','orc'])
with col2:
    domn_nm = st.selectbox("Domain Name", 
    [
    "fin_user",
    "smrt_fctry",
    "comrcl_user",
    "keu_bw",
    "eu_nielsen",
    "anaplan",
    "kg_bw_bpc",
    "tpm_optmztn",
    "b_and_m_user",
    "kna_ecc",
    "keu_ecc",
    "keu_scm",
    "sc_user",
    "sif",
    "eu_ktie",
    "sales_prfmnc_eval",
    "mktg_start_plan",
    "info_sec_mgmt",
    "vndr_mstr",
    "cust_mstr",
    "qual_mgmt",
    "sales_exec",
    "sales_order_mgmt",
    "sc_plan",
    "prcurmt",
    "sales_strat_plan",
    "fndtn",
    "log_exec",
    "matrl_mstr",
    "curr_mstr",
    "fin_plan_analys",
    "price_allwnc_mstr",
    "log",
    "loc_mstr",
    "fin_acctg_ops"]
    )
    delmtr_cd = st.text_input("Delimiter Code")
with col3:
    dataset_nm = st.text_input("Dataset Name")
    dprct_methd_cd = st.selectbox("Deprecation Method", ["okrdra",
                                                        "orrrra",
                                                        "okkkra",
                                                        "okkkra_extrctr",
                                                        "okkkka",
                                                        "file_cntl_upsrt_repl"])
with col4:
    table_nm = st.text_input("Table Name")
    data_clasfctn_nm = st.selectbox("Data Classification Name", ["confd","intrnl"])
with col5:
    dialect = st.selectbox("Database", ["Snowflake","Redshift"])
    warehouse_nm = st.selectbox("Warehouse", ["keu_it_small",
                                                    "keu_fin_small",
                                                    "keu_elt_analytic_small",
                                                    "keu_elt_supplychain_small"])



uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])



def extract_metadata_from_excel(xls):
    sheet_name = xls.sheet_names[0]  # Read first sheet
    df = xls.parse(sheet_name, dtype=str)  # Read data as strings

    if df.empty:
        return pd.DataFrame()  # Return empty DataFrame if no data

    df.columns = df.columns.str.strip()  # Normalize column names

    extracted_data = []
    column_counter = 1
    for col in df.columns:
        col_data = df[col].dropna()  # Remove NaN values

        inferred_type = infer_snowflake_type(col_data)
        max_length = col_data.astype(str).str.len().max() if not col_data.empty else 0
        primary_key = "X" if col_data.nunique() == len(df) and not col_data.empty else ""

        extracted_data.append({
            "src_nm" : src_nm,
            "src_table_nm" : table_nm,
            "field_nm" : col,
            "field_posn_nbr" : column_counter, 
            "datatype_nm" : inferred_type,
            "datatype_size_val" : max_length if max_length else "",
            "datatype_scale_val" : max_length if max_length else "",
            "key_ind" : primary_key,
            "check_table" : '',
            "field_desc" : '',
            "dprct_ind" : '',
            "partitn_ind" : '',
            "sort_key_ind" : '',
            "dist_key_ind" : '',
            "proc_stage_cd" : '',
            "catlg_flg" : '',
            "dblqt_repl_flg" : '',
            "delta_key_ind": ''
        })
        column_counter += 1

    return pd.DataFrame(extracted_data)

def generate_sys_config_dataset_info():
    extracted_data = []
    extracted_data.append(
    {
    "src_nm": src_nm,
    "dataset_nm": dataset_nm,
    "file_qty": 1,
    "file_range_min": 30,
    "pre_proc_flg": "N",
    "serv_now_priorty_cd": "P4",
    "sla_runtm_second": 0,
    "serv_now_group_nm": "kortex_nga_aws.global.l2",
    "err_notfcn_email_nm": "Non-Production_Kortex_AWS_Alerts@kellogg.com",
    "notfcn_email_nm": "",
    "virt_env_cd": "preproc",
    "proc_stage_cd": "Y",
    "catlg_flg": "Y",
    "trgt_dw_list": dialect,
    "cmput_whse_nm": warehouse_nm,
    "manl_upld_s3_uri_txt": "",
    "manl_upld_flg":"N"
    }
    )
    return pd.DataFrame(extracted_data)
    
def generate_sys_config_pre_proc_info():
    extracted_data = []
    extracted_data.append(
    {
    "src_nm": src_nm,
    "dataset_nm": dataset_nm,
    "file_patrn_txt": "",
    "pre_proc_methd_val": "",
    "file_qty": 0,
    "fmt_type_cd": ""
    }
    )
    return pd.DataFrame(extracted_data)
    
def generate_sys_config_table_info():
    extracted_data = []
    extracted_data.append(
    {
    "src_nm": src_nm,
    "domn_nm": domn_nm,
    "dataset_nm": dataset_nm,
    "redshift_table_nm": table_nm,
    "src_table_nm": table_nm,
    "data_clasfctn_nm": data_clasfctn_nm,
    "file_qty": 0,
    "fmt_type_cd": fmt_type_cd,
    "delmtr_cd": delmtr_cd,
    "file_patrn_txt": "",
    "dprct_methd_cd": dprct_methd_cd,
    "load_enbl_flg": "Y",
    "file_hdr_cnt": 1,
    "pre_proc_flg": "N",
    "pre_proc_cd": "",
    "src_encod_cd": "",
    "src_chrset_cd": "",
    "src_newln_chr_cd": "",
    "proc_stage_cd": "load",
    "catlg_flg": "Y",
    "dprct_selct_critra_txt": "",
    "bypas_file_order_seq_check_ind": "N",
    "land_spctrm_flg": "",
    "wrkflw_nm": "",
    "copy_by_field_nm_not_posn_ind": "",
    "bypas_file_hdr_config_posn_check_ind": "",
    "bypas_file_hdr_config_check_ind": ""
    }
    )
    return pd.DataFrame(extracted_data)

def generate_create_table_script(metadata_df,schema_name):
    if metadata_df.empty:
        return "No metadata available to generate SQL."
    
    table_name = metadata_df.iloc[0]['src_table_nm']
    columns_sql = []
    
    for _, row in metadata_df.iterrows():
        col_def = f"{row['field_nm']} {row['datatype_nm']}"
        
        # Add primary key if applicable
        if row['key_ind'] == "Y":
            col_def += " PRIMARY KEY"
        
        columns_sql.append(col_def)
    
    sql_script = (
        f"CREATE TABLE {schema_name}.{src_nm}_{dataset_nm}_{table_name} (\n"
        + ",\n".join(columns_sql) +
        "\n);"
    )
    
    return sql_script

# Ensure session state variables exist
if "template_generated" not in st.session_state:
    st.session_state.template_generated = False
if "metadata_df" not in st.session_state:
    st.session_state.metadata_df = None
if "sheets_data" not in st.session_state:
    st.session_state.sheets_data = {}
if "sql_scripts" not in st.session_state:
    st.session_state.sql_scripts = {"land": "", "stage": "", "rds": ""}
# Ensure session state for template generation
if "template_generated" not in st.session_state:
    st.session_state.template_generated = False
if "sql_generated" not in st.session_state:
    st.session_state.sql_generated = False

# Button to generate template
generate_template = st.button("Generate Template")

if generate_template:

    if uploaded_file is None :
        st.error("Upload Sample Data File")
    else : 
        st.session_state.template_generated = True  # Persist template generation

        
# Display template data if available
if st.session_state.template_generated:
    xls = pd.ExcelFile(uploaded_file)
    sheet_names = [
        "sys_config_dataset_info",
        "sys_config_pre_proc_info",
        "sys_config_table_info",
        "sys_config_table_field_info",
    ]
    st.session_state.sheets_data = {sheet: xls.parse(sheet) if sheet in xls.sheet_names else pd.DataFrame() for sheet in sheet_names}
    tab1, tab2, tab3, tab4 = st.tabs(sheet_names)
    st.session_state.metadata_df = extract_metadata_from_excel(xls)
    st.session_state.sys_config_dataset_info = generate_sys_config_dataset_info()
    st.session_state.sys_config_pre_proc_info = generate_sys_config_pre_proc_info()
    st.session_state.sys_config_table_info = generate_sys_config_table_info()
    def update_dataset_info():
        st.write("update dataset info - called!")
        st.write(st.session_state.get("sys_config_dataset_info_edit"))
        st.session_state.sys_config_dataset_info = st.session_state.sys_config_dataset_info_edit

    def update_pre_proc_info():
        st.session_state.sys_config_proc_info = st.session_state.sys_config_pre_proc_info_edit

    def update_table_info():
        st.session_state.sys_config_table_info = st.session_state.sys_config_table_info_edit

    def update_field_info():
        st.session_state.metadata_df = st.session_state.sys_config_table_field_info_edit

    with tab1:
        st.write(f"### {sheet_names[0]}")
        st.data_editor(st.session_state.sys_config_dataset_info, 
        num_rows="dynamic",  # Allows adding new rows
        key="sys_config_dataset_info_edit",
        on_change=update_dataset_info)

    with tab2:
        st.write(f"### {sheet_names[1]}")
        st.data_editor(st.session_state.sys_config_pre_proc_info, 
        num_rows="dynamic",  # Allows adding new rows
        key="sys_config_pre_proc_info_edit",
        on_change=update_pre_proc_info)

    with tab3:
        st.write(f"### {sheet_names[2]}")
        st.data_editor(st.session_state.sys_config_table_info, 
        num_rows="dynamic",  # Allows adding new rows
        key="sys_config_table_info_edit",
        on_change=update_table_info)

    with tab4:
        st.write(f"### {sheet_names[3]}")
        st.data_editor(st.session_state.metadata_df, 
        num_rows="dynamic",  # Allows adding new rows
        key="sys_config_table_field_info_edit",
        on_change=update_field_info)

def generate_insert_statements():
    insert_statements = []

    # Helper function to generate an INSERT statement
    def create_insert_statement(table_name, df):
        if df.empty:
            return f"-- No data to insert into {table_name}"
        
        columns = ", ".join(df.columns)
        values = ",\n".join(
            f"({', '.join(repr(v) if pd.notna(v) else 'NULL' for v in row)})"
            for row in df.itertuples(index=False, name=None)
        )
        return f"INSERT INTO {table_name} ({columns}) VALUES\n{values};"

    # Dataset Info
    insert_statements.append(create_insert_statement("app_mgmt.sys_config_dataset_info", st.session_state.sys_config_dataset_info))

    # Pre Proc Info
    insert_statements.append(create_insert_statement("app_mgmt.sys_config_pre_proc_info", st.session_state.sys_config_pre_proc_info))

    # Table Info
    insert_statements.append(create_insert_statement("app_mgmt.sys_config_table_info", st.session_state.sys_config_table_info))

    # Table Field Info
    insert_statements.append(create_insert_statement("app_mgmt.sys_config_table_field_info", st.session_state.metadata_df))

    # Store all statements in session state
    st.session_state.rds_sql_script = "\n\n".join(insert_statements)  

if st.session_state.template_generated:
    generate_sql_button = st.button("Generate SQL Scripts")

    if generate_sql_button:

        if st.session_state.metadata_df.empty:
            st.error("Metadata DataFrame is empty! SQL cannot be generated.")
        else:
            st.session_state.sql_generated = True
if st.session_state.sql_generated :           
    sheet_names_sql = ["Land DDL", "Stage DDL", "RDS Config Entries"]
    tab1, tab2, tab3 = st.tabs(sheet_names_sql)

    try:
        with tab1:
            st.session_state.land_sql_script = generate_create_table_script(st.session_state.metadata_df, 'land')
            st.text_area("Generated Land DDL Scripts", st.session_state.land_sql_script, height=200)
            #st.write(f"Generated SQL for Land: {land_sql_script[:500]}")  # Show first 500 chars

        with tab2:
            st.session_state.stage_sql_script = generate_create_table_script(st.session_state.metadata_df, 'stage')
            st.text_area("Generated Stage DDL Scripts", st.session_state.stage_sql_script, height=200)
            #st.write(f"Generated SQL for Stage: {stage_sql_script[:500]}")

        with tab3:
            # Apply only edited rows to the DataFrame
            if "sys_config_dataset_info_edit" in st.session_state:
                edited_rows = st.session_state.sys_config_dataset_info_edit.get("edited_rows", {})
                for idx, changes in edited_rows.items():
                    for col, value in changes.items():
                        st.session_state.sys_config_dataset_info.at[int(idx), col] = value

            if "sys_config_pre_proc_info_edit" in st.session_state:
                edited_rows = st.session_state.sys_config_pre_proc_info_edit.get("edited_rows", {})
                for idx, changes in edited_rows.items():
                    for col, value in changes.items():
                        st.session_state.sys_config_pre_proc_info.at[int(idx), col] = value

            if "sys_config_table_info_edit" in st.session_state:
                edited_rows = st.session_state.sys_config_table_info_edit.get("edited_rows", {})
                for idx, changes in edited_rows.items():
                    for col, value in changes.items():
                        st.session_state.sys_config_table_info.at[int(idx), col] = value

            if "sys_config_table_field_info_edit" in st.session_state:
                edited_rows = st.session_state.sys_config_table_field_info_edit.get("edited_rows", {})
                for idx, changes in edited_rows.items():
                    for col, value in changes.items():
                        st.session_state.sys_config_table_field_info.at[int(idx), col] = value

            # Now generate insert statements using the updated DataFrames
            generate_insert_statements()

            st.text_area("Generated RDS Scripts", st.session_state.rds_sql_script, height=200)

            #st.write(f"Generated SQL for RDS: {rds_sql_script[:500]}")
        st.session_state.sql_generated = True

    except Exception as e:
        st.error(f"SQL generation failed: {e}")
        st.text(traceback.format_exc())
if st.session_state.sql_generated : 
    
    col1, col2,col3,col4,col5 = st.columns([1, 1, 1, 1, 1])

    # Add buttons inside each column
    with col1:
        download_button_clicked = st.button("Download Files", use_container_width=True)

    with col2:
        git_push_clicked = st.button("GIT Push", use_container_width=True)
        
    if git_push_clicked:
        files_to_upload = []
        branch_name = f"feature/{src_nm}_{dataset_nm}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        folder_name = f"{src_nm}_{dataset_nm}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        save_sql_files_locally(src_nm, dataset_nm, st.session_state.land_sql_script, st.session_state.stage_sql_script, st.session_state.rds_sql_script)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        temp_dir = os.path.join(script_dir, "temp")

        # Create a list of SQL files dynamically
        sql_files_list = [
            os.path.join(temp_dir, f"{src_nm}_{dataset_nm}_land.sql"),
            os.path.join(temp_dir, f"{src_nm}_{dataset_nm}_stage.sql"),
            os.path.join(temp_dir, f"{src_nm}_{dataset_nm}_rds.sql"),
]
        result = git_push_files_to_feature_branch(sql_files_list,branch_name,folder_name)
        st.success("Git Code Push Successfull and PR raised")

    if download_button_clicked:
        zip_data = create_downloadable_zip(
            st.session_state.sheets_data, src_nm, dataset_nm, st.session_state.land_sql_script, st.session_state.stage_sql_script, st.session_state.rds_sql_script
        )

        st.download_button(
            label="Download Files",
            data=zip_data,
            file_name=f"{src_nm}_{dataset_nm}_Onboarding_Files.zip",
            mime="application/zip"
        )
