# http://127.0.0.1:8000/#/

import asyncio
import logging
import os
from datetime import datetime
from io import BytesIO
import traceback
import subprocess
from dotenv import load_dotenv
from docxtpl import DocxTemplate
from docx2pdf import convert
from app.core.utils.db_utils import *
from app.core.analysis.report_generation_submodules.utilities import *
import io
from app.core.security.jwt import create_jwt_token
from app.core.config import get_settings
import tempfile
import requests 
import json
from docx.shared import Mm
from docxtpl import DocxTemplate, InlineImage
from app.core.analysis.report_generation_submodules.populate import *
from .summarization import sape_summary
from .summarization import bcf_summary
from .summarization import state_ownership_summary
from .summarization import financials_summary
from .summarization import adverse_media_summary
from .summarization import cybersecurity_summary
from .summarization import esg_summary
from .summarization import legal_regulatory_summary
from .summarization import overall_summary
# import nltk
# nltk.download('punkt')  # Download required dataset
# from nltk.tokenize import sent_tokenize
load_dotenv()
def remove_time_keys(obj):
    if isinstance(obj, dict):
        # Remove 'create_time' and 'update_time' if they exist
        obj.pop('create_time', None)
        obj.pop('update_time', None)
        # Recursively clean any nested dictionaries
        for key, value in obj.items():
            obj[key] = remove_time_keys(value)
    elif isinstance(obj, list):
        # Recursively clean any elements in the list
        for i in range(len(obj)):
            obj[i] = remove_time_keys(obj[i])
    return obj

async def report_generation(data, session, upload_to_blob:bool, session_outputs:bool, ts_data=None):
    incoming_ens_id = data["ens_id"]
    incoming_country = data["country"]
    incoming_name = data["name"]
    session_id = data["session_id"]
    # national_id = data["national_id"]

    template = r"app\core\analysis\report_generation_submodules\template_A.docx"
    output_folder = r"app\core\analysis\report_generation_submodules\output"

    # def generate_and_upload_pdf(template_path, context, session_id):
    #     """
    #     Generates a DOCX from a template, converts it to PDF, and uploads it to Azure Blob Storage.

    #     :param template_path: Path to the DOCX template file.
    #     :param context: Dictionary containing values to render in the template.
    #     :param session_id: Used as the Azure Blob container name.
    #     :return: True if upload is successful, False otherwise.
    #     """

    #     # Load and render the DOCX template
    #     doc = DocxTemplate(template_path)
    #     doc.render(context)

    #     # Save DOCX temporarily
    #     with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as temp_docx:
    #         doc.save(temp_docx.name)  # Save rendered DOCX file
    #         temp_pdf = temp_docx.name.replace(".docx", ".pdf")  # Define PDF path

    #         # Convert DOCX to PDF
    #         convert(temp_docx.name, temp_pdf)

    #     # Convert PDF file to BytesIO buffer
    #     with open(temp_pdf, "rb") as pdf_file:
    #         pdf_buffer = BytesIO(pdf_file.read())  # Read file into BytesIO

    #     # Upload the PDF to Azure Blob Storage
    #     file_name = os.path.basename(temp_pdf)
    #     upload_success = upload_to_azure_blob(pdf_buffer, file_name, session_id)

    #     return upload_success


    def get_day_with_suffix(day):
        if 11 <= day <= 13:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
        return f"{day}{suffix}"

    try:

        logging.info(f"====== Begin: Reports for supplier. Saving locally: {session_outputs} ======")
        count = 0
        results = {}

        # Make sure the table that is referenced here has unique supplier records #TODO WHAT IS THIS FOR
        required_columns = ["name", "country"]
        supplier_data = await get_dynamic_ens_data("supplier_master_data", required_columns=required_columns, ens_id=incoming_ens_id, session_id=session_id, session=session)
        print("\n\nSupplier data >>>> \n\n", supplier_data)

        process_details = {
            "ens_id": incoming_ens_id,
            "supplier_name": incoming_name,
            "L2_supplier_name_validation": "",
            "local": {
                "session_outputs": session_outputs,
                "docx": "NA",
                "pdf": "NA"
            },
            "blob": {
                "upload_to_blob": upload_to_blob,
                "docx": "NA",
                "pdf": "NA"
            },
            "populate_sections": {
                "profile":"",
                "sanctions": "",
                "anti_brib_corr":"",
                "gov_ownership":"",
                "financial":"",
                "adv_media":"",
                "cybersecurity":"",
                "esg":"",
                "regulatory_and_legal":""
            }
        }

        logging.info(f"--> Generating reports for ID: {incoming_ens_id}")

        # Format the date
        current_date = datetime.now()
        formatted_date = f"{get_day_with_suffix(current_date.day)} {current_date.strftime('%B')}, {current_date.year}"

        # Generate the plot
        sentiment_data_agg = [
            {"month": "January", "negative": 5},
            {"month": "February", "negative": 3},
            {"month": "March", "negative": 8},
            {"month": "April", "negative": 4},
            {"month": "May", "negative": 6}
        ]

        context = {}

        sanctions = await sape_summary(data, session)
        bcf = await bcf_summary(data, session)
        sco = await state_ownership_summary(data, session)
        financials = await financials_summary(data, session)
        adverse_media = await adverse_media_summary(data, session)
        cyber = await cybersecurity_summary(data, session)
        esg = await esg_summary(data, session)
        regal = await legal_regulatory_summary(data, session)
        summary = await overall_summary(data, session, supplier_name=incoming_name)
        static_entries = {
            'date': formatted_date,
            'risk_level': "Medium",
            'summary_of_findings': summary,
            'sanctions_summary': sanctions,
            'anti_summary': bcf,
            'gov_summary': sco,
            'financial_summary': financials,
            'adv_summary': adverse_media,
            'cyber_summary': cyber,
            'esg_summary': esg,
            'ral_summary': regal
        }

        context.update(static_entries)


        # Get ratings for all sections
        ratings_data = await get_dynamic_ens_data(
            "ovar",
            required_columns=["all"],
            ens_id=incoming_ens_id,
            session_id=session_id,
            session=session
            )

        if ratings_data:
            for row in ratings_data:
                if row.get("ens_id") == incoming_ens_id and row.get("session_id") == session_id:
                    if row.get("kpi_code") == "sanctions" and row.get("kpi_area") == "theme_rating":
                        context["sanctions_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "bribery_corruption_overall" and row.get("kpi_area") == "theme_rating":
                        context["anti_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "government_political" and row.get("kpi_area") == "theme_rating":
                        context["gov_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "financials" and row.get("kpi_area") == "theme_rating":
                        context["financial_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "other_adverse_media" and row.get("kpi_area") == "theme_rating":
                        context["adv_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "cyber" and row.get("kpi_area") == "theme_rating":
                        context["cyber_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "esg" and row.get("kpi_area") == "theme_rating":
                        context["esg_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "regulatory_legal" and row.get("kpi_area") == "theme_rating":
                        context["regulatory_and_legal_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "supplier" and row.get("kpi_area") == "overall_rating":
                        context["risk_level"] = row.get("kpi_rating")
        else:
            no_ratings = {
                "sanctions_rating":"None",
                "gov_rating":"None",
                "anti_rating": "None",
                "financial_rating":"None",
                "adv_rating":"None",
                "cyber_rating":"None",
                "esg_rating":"None",
                "regulatory_and_legal_rating":"None",
                "risk_level":"None"
            }
            context.update(no_ratings)

        context["name"] = incoming_name

############################################################################################################################

        # Profile Data
        profile_data = await populate_profile(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
        print(f"profile type {type(profile_data)}")
        # print("profile_data", profile_data)
        # print("Company Name:", profile_data["name"])

        context["location"] = profile_data["location"]
        context["address"] = profile_data["address"]
        context["website"] = profile_data["website"]
        context["active_status"] = profile_data["active_status"]
        context["operation_type"] = profile_data["operation_type"]
        context["legal_status"] = profile_data["legal_status"]
        context["national_id"] = profile_data["national_identifier"]
        context["alias"] = profile_data["alias"]
        context["incorporation_date"] = profile_data["incorporation_date"]
        context["subsidiaries"] = profile_data["subsidiaries"]
        context["corporate_group"] = profile_data["corporate_group"]
        context["shareholders"] = profile_data["shareholders"]
        context["key_exec"] = profile_data["key_executives"]
        context["revenue"] = profile_data["revenue"]
        context["employee_count"] = profile_data["employee"]

############################################################################################################################

        # Sanctions DataFrames
        try:
            data = await populate_sanctions(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            temp = data["sanctions"]
            if not temp.empty:
                sape_data = temp.to_dict(orient='records')
                context["sanctions_findings"] = True
                context["sape_data"] = sape_data
            else:
                context["sanctions_findings"] = False
            # PeP Dataframes
            data = await populate_pep(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            temp = data["pep"]
            if not temp.empty:
                pep_info = temp.to_dict(orient='records')
                context["pep_findings"] = True
                context["pep_data"] = pep_info
            else:
                context["pep_findings"] = False
            process_details["populate_sections"]["sanctions"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["sanctions"] = str(tb)

############################################################################################################################

        try:
            # Anti-Corruption DataFrames
            anti_corruption_data = await populate_anti(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            bribery_df = anti_corruption_data["bribery"]
            corruption_df = anti_corruption_data["corruption"]
            if not bribery_df.empty:
                temp = anti_corruption_data["bribery"]
                bribery_data = temp.to_dict(orient='records')
                context["bribery_findings"] = True
                context["bribery_data"] = bribery_data
            else:
                context["bribery_findings"] = False
            if not corruption_df.empty:
                temp = anti_corruption_data["corruption"]
                corruption_data = temp.to_dict(orient='records')
                context["corruption_findings"] = True
                context["corruption_data"] = corruption_data
            else:
                context["corruption_findings"] = False
            process_details["populate_sections"]["anti_brib_corr"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["anti_brib_corr"] = str(tb)
        # print("Bribery Data:\n", bribery_df)
        # print("Corruption Data:\n", corruption_df)

############################################################################################################################

        try:
            # Financials DataFrames
            financials_data_1 = await populate_financials_risk(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            financials_data_2 = await populate_financials_value(incoming_ens_id=incoming_ens_id,incoming_session_id=session_id, session=session)
            financial_df_1 = financials_data_1["financial"]
            financial_df_2 = financials_data_2["financial"]
            # bankruptcy_df = financials_data["bankruptcy"]
            if not financial_df_1.empty:
                temp = financials_data_1["financial"]
                financial_data_1 = temp.to_dict(orient='records')
                context["financial_findings"] = True
                context["financial_data"] = financial_data_1
            elif not financial_df_2.empty:
                temp = financials_data_2["financial"]
                financial_data_2 = temp.to_dict(orient='records')
                context["financial_findings"] = True
                context["financial_data"] = financial_data_2
            else:
                context["financial_findings"] = False
            # if not bankruptcy_df.empty:
            #     temp = financials_data["bankruptcy"]
            #     bankruptcy_data = temp.to_dict(orient='records')
            #     print(f"\n\nBankruptcy data:\n {bankruptcy_data}")
            #     context["bankruptcy_findings"] = True
            #     context["bankruptcy_data"] = bankruptcy_data
            # else:
            #     context["bankruptcy_findings"] = False
            process_details["populate_sections"]["financial"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["financial"] = str(tb)
        # print("Financial Data:\n", financial_df)
        # print("Bankruptcy Data:\n", bankruptcy_df)

############################################################################################################################

        try:
            # Ownership DataFrame
            ownership_data = await populate_ownership(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            state_ownership_df = ownership_data["state_ownership"]
            if not state_ownership_df.empty:
                temp = ownership_data["state_ownership"]
                sown_data = temp.to_dict(orient='records')
                context["sown_findings"] = True
                context["sown_data"] = sown_data
            else:
                context["sown_findings"] = False
            process_details["populate_sections"]["gov_ownership"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["gov_ownership"] = str(tb)
        # print("State Ownership Data:\n", state_ownership_df)

############################################################################################################################

        try:
            print("country started")
            # Country Risk DataFrame
            country_risk_data = await populate_country_risk(incoming_ens_id=incoming_ens_id,
                                                      incoming_session_id=session_id, session=session)
            country_risk_df = country_risk_data["country_risk"]
            if not country_risk_df.empty:
                temp = country_risk_data["state_ownership"]
                country_risk_data = temp.to_dict(orient='records')
                context["country_risk_findings"] = True
                context["country_risk_data"] = country_risk_data
            else:
                context["country_risk_findings"] = False
            process_details["populate_sections"]["country_risk"] = "success"
            print("Country Risk Data:\n", country_risk_df)
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["country_risk"] = str(tb)

############################################################################################################################

        try:
            # Ownership flag DataFrame
            print("flag started")
            ownership_flag_data = await populate_ownership_flag(incoming_ens_id=incoming_ens_id,
                                                            incoming_session_id=session_id, session=session)
            ownership_flag_df = ownership_flag_data["ownership_flag"]
            if not ownership_flag_df.empty:
                temp = ownership_flag_data["state_ownership"]
                ownership_flag_data = temp.to_dict(orient='records')
                context["ownership_flag_findings"] = True
                context["ownership_flag_data"] = ownership_flag_data
            else:
                context["ownership_flag_findings"] = False
            process_details["populate_sections"]["ownership_flag"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["ownership_flag"] = str(tb)
        # print("State Ownership Data:\n", state_ownership_df)

############################################################################################################################

        try:
            # Other adverse media
            oam = await populate_other_adv_media(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            state_ownership_df = oam["adv_media"]
            if not state_ownership_df.empty:
                temp = oam["adv_media"]
                adv_data = temp.to_dict(orient='records')
                context["adv_findings"] = True
                context["adv_data"] = adv_data
            else:
                context["adv_findings"] = False
            process_details["populate_sections"]["adv_media"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["adv_media"] = str(tb)

        # Generate graphs
        def generate_graph(data):
            # Sample DataFrame
            df = pd.DataFrame(data)

            # Create a bar plot
            plt.figure(figsize=(6, 4))
            plt.bar(df['Category'], df['Values'], color='skyblue')
            plt.xlabel('Category')
            plt.ylabel('Values')
            plt.title('Sample Bar Chart')
            # Save the plot to a BytesIO object
            img_stream = io.BytesIO()
            plt.savefig(img_stream, format='png', bbox_inches='tight')
            plt.close()  # Close the figure to free memory
            # Move the cursor to the beginning of the BytesIO stream
            img_stream.seek(0)
            return img_stream
        
        news_data1 = {'Category': ['A', 'B', 'C', 'D'], 'Values': [10, 25, 7, 18]}
        img_stream1 = generate_graph(data=news_data1)
        news_data2 = {'Catagory': ['A', 'B', 'C', 'D'], 'Values': [5, 8, 7, 23]}
        img_stream2 = generate_graph(data=news_data2)

        # Adding graphs
        graphs = []
        image1 = InlineImage(doc, img_stream1, width=Mm(100))
        graphs.append(image1)
        image2 = InlineImage(doc, img_stream2, width=Mm(100))
        graphs.append(image2)

        # context["gap"] = True
        # context["graphs"] = graphs


############################################################################################################################

        try:
            # Regulatory and Legal
            ral = await populate_regulatory_legal(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            reg_df = ral["reg_data"]
            leg_df = ral["legal_data"]
            if not reg_df.empty:
                temp = ral["reg_data"]
                regulatory_data = temp.to_dict(orient='records')
                context["reg_findings"] = True
                context["reg_data"] = regulatory_data
            else:
                context["reg_findings"] = False
            if not leg_df.empty:
                temp = ral["legal_data"]
                legal_data = temp.to_dict(orient='records')
                context["leg_findings"] = True
                context["leg_data"] = legal_data
            else:
                context["leg_findings"] = False
            process_details["populate_sections"]["regulatory_and_legal"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["regulatory_and_legal"] = str(tb)

############################################################################################################################

        try:
            # Cybersecurity DataFrame
            cybersecurity_data = await populate_cybersecurity(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            cybersecurity_df = cybersecurity_data["cybersecurity"]
            if not cybersecurity_df.empty:
                temp = cybersecurity_data["cybersecurity"]
                cyb_data = temp.to_dict(orient='records')
                context["cyb_findings"] = True
                context["cyb_data"] = cyb_data
            else:
                context["cyb_findings"] = False
            process_details["populate_sections"]["cybersecurity"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["cybersecurity"] = str(tb)
        # print("Cybersecurity Data:\n", cybersecurity_df)

############################################################################################################################

        try:
            # ESG DataFrame
            esg_data = await populate_esg(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            esg_df = esg_data["esg"]
            if not esg_df.empty:
                temp = esg_data["esg"]
                esgdata = temp.to_dict(orient='records')
                context["esg_findings"] = True
                context["esg_data"] = esgdata
            else:
                context["esg_findings"] = False
            process_details["populate_sections"]["esg"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["esg"] = str(tb)
        # print("ESG Data:\n", esg_df)

############################################################################################################################

        # Initialize the document template
        doc = DocxTemplate(template)
        # Fetch `ts_flag` from ts_data
        if ts_data:
            matching_entry = next((entry for entry in ts_data["results"] if entry["ens_id"] == incoming_ens_id), None)
            if matching_entry:
                process_details["L2_supplier_name_validation"] = matching_entry["verification_details"]["is_verified"]
                context['ts_flag'] = matching_entry["verification_details"]["is_verified"]
        else:
            process_details["L2_supplier_name_validation"] = False

        doc.render(context)
        # Save DOCX to buffer
        docx_buffer = BytesIO()
        doc.save(docx_buffer)
        docx_buffer.seek(0)


        # # Convert DOCX to PDF and save to buffer
        # pdf_buffer = io.BytesIO()
        # with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as temp_docx_file:
        #     temp_docx_file.write(docx_buffer.getvalue())
        #     temp_docx_file.seek(0)  # Ensure it's ready for reading
        #     # Convert to PDF and write the PDF to a buffer
        #     pdf_path = temp_docx_file.name.replace(".docx", ".pdf")
        #     convert(temp_docx_file.name, pdf_path)
        #     # Read the generated PDF back into a buffer
        #     with open(pdf_path, "rb") as pdf_file:
        #         pdf_buffer.write(pdf_file.read())

        # Save the DOCX and PDF files directly into the output folder
        docx_file = f"{incoming_ens_id}/{incoming_name}.docx"
        pdf_file = f"{incoming_ens_id}/{incoming_name}.pdf"

        docx_file_local = f"{session_id}_{incoming_ens_id}_{incoming_name}.docx"
        pdf_file_local = f"{session_id}_{incoming_ens_id}_{incoming_name}.pdf"

        # Ensure the output folder exists
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            logging.info(f"Created output folder at: {output_folder}")
        else:
            # Clear the folder by removing all files inside it
            for file in os.listdir(output_folder):
                file_path = os.path.join(output_folder, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            logging.info(f"Output folder cleared: Awaiting new files...")

        docx_path = os.path.join(output_folder, docx_file_local)
        pdf_path = os.path.join(output_folder, pdf_file_local)

        # Save locally if requested
        if session_outputs:
            # Save DOCX file to output folder
            logging.info(f"Saving {docx_path} locally...")
            try:
                with open(docx_path, "wb") as f:
                    f.write(docx_buffer.getvalue())
                logging.info(f"Saved DOCX report at {docx_path}")
                process_details["local"]["docx"] = "success"
            except Exception as e:
                process_details["local"]["docx"] = "failed"
                process_details["local"]["docx_error"] = str(e)

            # Convert DOCX to PDF using docx2pdf
            try:
                docx_path_for_pdf = os.path.join(output_folder, docx_file_local)
                convert(docx_path_for_pdf, pdf_path)
                logging.info(f"Saved PDF report at {pdf_path}")
                process_details["local"]["pdf"] = "success"
            except Exception as e:
                process_details["local"]["pdf"] = "failed"
                process_details["local"]["pdf_error"] = str(e)

        # Upload to Azure Blob if requested
        if upload_to_blob:
            # Upload DOCX buffer
            try:
                docx_upload_success = upload_to_azure_blob(docx_buffer, docx_file, session_id)
                process_details["blob"]["docx"] = "success" if docx_upload_success else "failed"
            except Exception as e:
                process_details["blob"]["docx"] = "failed"
                process_details["blob"]["docx_error"] = str(e)

            # Upload PDF buffer
            try:
                # status = generate_and_upload_pdf(template_path=template, context=context, session_id=session_id)
                with open(pdf_path, "rb") as file:
                    pdf_buffer_ = BytesIO(file.read())
                pdf_buffer_.seek(0)
                status = upload_to_azure_blob(pdf_buffer_, pdf_file, session_id)
                process_details["blob"]["pdf"] = "success" if status else "failed"
            except Exception as e:
                process_details["blob"]["pdf"] = "failed"
                process_details["blob"]["pdf_error"] = str(e)

        count += 1

        logging.info(f"====== End: Generated reports for supplier ======")

        output = {
            "status": "success" if process_details["blob"]["docx"] == "True" or process_details["local"]["docx"] == "True" else "failure",
            "message": f"Generation of a report for supplier ens id - {incoming_ens_id}",
            "data": process_details
        }

        return True, process_details

    except Exception as e:
        tb = traceback.format_exc()  # Capture the full traceback

        process_details = {
            "ens_id": incoming_ens_id,
            "supplier_name": incoming_name,
            "L2_supplier_name_validation": "",
            "error": f"ReportGenerationCode - {str(e)}",
            "traceback": tb  # Add detailed traceback info
        }

        logging.error(f"Process details: {process_details}")  # Use logging.error for exceptions

        return False, process_details
    
async def report_generation_poc(data, session, upload_to_blob: bool, save_locally: bool, ts_data=None):
    incoming_ens_id = data["ens_id"]
    incoming_country = data["country"]
    incoming_name = data["name"]
    session_id = data["session_id"]
    # national_id = data["national_id"]



    def get_day_with_suffix(day):
        if 11 <= day <= 13:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
        return f"{day}{suffix}"

    try:

        logging.info(f"====== Begin: Reports for supplier. Saving locally: {save_locally} ======")
        count = 0
        results = {}

        # Make sure the table that is referenced here has unique supplier records #TODO WHAT IS THIS FOR
        required_columns = ["name", "country"]
        supplier_data = await get_dynamic_ens_data("upload_supplier_master_data", required_columns=required_columns,
                                                   ens_id=incoming_ens_id, session_id=session_id, session=session)
        print("\n\nSupplier data >>>> \n\n", supplier_data)

        process_details = {
            "ens_id": incoming_ens_id,
            "supplier_name": incoming_name,
            "L2_supplier_name_validation": "",
            "local": {
                "save_locally": save_locally,
                "docx": "NA",
                "pdf": "NA"
            },
            "blob": {
                "upload_to_blob": upload_to_blob,
                "docx": "NA",
                "pdf": "NA"
            },
            "populate_sections": {
                "profile": "",
                "sanctions": "",
                "anti_brib_corr": "",
                "gov_ownership": "",
                "financial": "",
                "adv_media": "",
                "cybersecurity": "",
                "esg": "",
                "regulatory_and_legal": "",
                "corporate_ownership": "",
            }
        }

        logging.info(f"--> Generating reports for ID: {incoming_ens_id}")

        # Format the date
        current_date = datetime.now()
        formatted_date = f"{get_day_with_suffix(current_date.day)} {current_date.strftime('%B')}, {current_date.year}"

        # Generate the plot
        sentiment_data_agg = [
            {"month": "January", "negative": 5},
            {"month": "February", "negative": 3},
            {"month": "March", "negative": 8},
            {"month": "April", "negative": 4},
            {"month": "May", "negative": 6}
        ]

        # summary = generate_summary(supplier_name=incoming_name)

        context = {}
        sanctions = await sape_summary(data, session)

        bcf = await bcf_summary(data, session)

        sco = await state_ownership_summary(data, session)

        financials = await financials_summary(data, session)

        adverse_media = await adverse_media_summary(data, session)

        cyber = await cybersecurity_summary(data, session)

        esg = await esg_summary(data, session)

        regal = await legal_regulatory_summary(data, session)

        summary = await overall_summary(data, session, supplier_name=incoming_name)


        static_entries = {
            'date': formatted_date,
            'risk_level': "Medium",
            'summary_of_findings': summary,
            'sanctions_summary': sanctions,
            'anti_summary': bcf,
            'gov_summary': sco,
            'financial_summary': financials,
            'adv_summary': adverse_media,
            'cyber_summary': cyber,
            'esg_summary': esg,
            'ral_summary': regal
        }
        context.update(static_entries)

        # Get ratings for all sections
        ratings_data = await get_dynamic_ens_data(
            "ovar",
            required_columns=["all"],
            ens_id=incoming_ens_id,
            session_id=session_id,
            session=session
        )

        if ratings_data:
            for row in ratings_data:
                if row.get("ens_id") == incoming_ens_id and row.get("session_id") == session_id:
                    if row.get("kpi_code") == "sanctions" and row.get("kpi_area") == "theme_rating":
                        context["sanctions_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "bribery_corruption_overall" and row.get("kpi_area") == "theme_rating":
                        context["anti_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "government_political" and row.get("kpi_area") == "theme_rating":
                        context["gov_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "financials" and row.get("kpi_area") == "theme_rating":
                        context["financial_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "other_adverse_media" and row.get("kpi_area") == "theme_rating":
                        context["adv_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "cyber" and row.get("kpi_area") == "theme_rating":
                        context["cyber_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "esg" and row.get("kpi_area") == "theme_rating":
                        context["esg_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "regulatory_legal" and row.get("kpi_area") == "theme_rating":
                        context["regulatory_and_legal_rating"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "supplier" and row.get("kpi_area") == "overall_rating":
                        context["risk_level"] = row.get("kpi_rating")
                    elif row.get("kpi_code") == "ownership_flag" and row.get("kpi_area") == "theme_rating":
                        context["corporate_ownership"] = row.get("kpi_rating")
        else:
            no_ratings = {
                "sanctions_rating": "None",
                "gov_rating": "None",
                "anti_rating": "None",
                "financial_rating": "None",
                "adv_rating": "None",
                "cyber_rating": "None",
                "esg_rating": "None",
                "regulatory_and_legal_rating": "None",
                "corporate_ownership":"None",
                "risk_level": "None"
            }
            context.update(no_ratings)

        context["name"] = incoming_name

        ############################################################################################################################

        # Profile Data
        profile_data = await populate_profile(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id,
                                              session=session)
        context["location"] = profile_data["location"]
        context["address"] = profile_data["address"]
        context["website"] = profile_data["website"]
        context["active_status"] = profile_data["active_status"]
        context["operation_type"] = profile_data["operation_type"]
        context["legal_status"] = profile_data["legal_status"]
        context["national_id"] = profile_data["national_identifier"]
        context["alias"] = profile_data["alias"]
        context["incorporation_date"] = profile_data["incorporation_date"]
        context["subsidiaries"] = profile_data["subsidiaries"]
        context["corporate_group"] = profile_data["corporate_group"]
        context["shareholders"] = profile_data["shareholders"]
        context["key_exec"] = profile_data["key_executives"]
        context["revenue"] = profile_data["revenue"]
        context["employee_count"] = profile_data["employee"]

        ############################################################################################################################

        # Sanctions DataFrames
        try:
            data = await populate_sanctions(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id,
                                            session=session)
            temp = data["sanctions"]
            if not temp.empty:
                sape_data = temp.to_dict(orient='records')
                sape_data= remove_time_keys(sape_data)
                context["sanctions_findings"] = True
                context["sape_data"] = sape_data
            else:
                context["sanctions_findings"] = False
            # PeP Dataframes
            # data = await populate_pep(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            # temp = data["pep"]
            # if not temp.empty:
            #     pep_info = temp.to_dict(orient='records')
            #     pep_info = remove_time_keys(pep_info)
            #     context["pep_findings"] = True
            #     context["pep_data"] = pep_info
            # else:
            #     context["pep_findings"] = False
            process_details["populate_sections"]["sanctions"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["sanctions"] = str(tb)

        ############################################################################################################################

        try:
            # Anti-Corruption DataFrames
            anti_corruption_data = await populate_anti(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id,
                                                       session=session)
            bribery_df = anti_corruption_data["bribery"]
            corruption_df = anti_corruption_data["corruption"]
            if not bribery_df.empty:
                temp = anti_corruption_data["bribery"]
                bribery_data = temp.to_dict(orient='records')
                bribery_data = remove_time_keys(bribery_data)
                context["bribery_findings"] = True
                context["bribery_data"] = bribery_data
            else:
                context["bribery_findings"] = False
            if not corruption_df.empty:
                temp = anti_corruption_data["corruption"]
                corruption_data = temp.to_dict(orient='records')
                corruption_data = remove_time_keys(corruption_data)
                context["corruption_findings"] = True
                context["corruption_data"] = corruption_data
            else:
                context["corruption_findings"] = False
            process_details["populate_sections"]["anti_brib_corr"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["anti_brib_corr"] = str(tb)


        ############################################################################################################################

        try:
            # Financials DataFrames
            financials_data_1 = await populate_financials_risk(incoming_ens_id=incoming_ens_id,
                                                               incoming_session_id=session_id, session=session)
            financials_data_2 = await populate_financials_value(incoming_ens_id=incoming_ens_id,
                                                                incoming_session_id=session_id, session=session)
            financial_df_1 = financials_data_1["financial"]
            financial_df_2 = financials_data_2["financial"]
            # bankruptcy_df = financials_data["bankruptcy"]
            if not financial_df_1.empty:
                temp = financials_data_1["financial"]
                financial_data_1 = temp.to_dict(orient='records')
                financial_data_1 = remove_time_keys(financial_data_1)
                context["financial_findings"] = True
                context["financial_data"] = financial_data_1
            elif not financial_df_2.empty:
                temp = financials_data_2["financial"]
                financial_data_2 = temp.to_dict(orient='records')
                financial_data_2 = remove_time_keys(financials_data_2)
                context["financial_findings"] = True
                context["financial_data"] = financial_data_2
            else:
                context["financial_findings"] = False
            # if not bankruptcy_df.empty:
            #     temp = financials_data["bankruptcy"]
            #     bankruptcy_data = temp.to_dict(orient='records')
            #     print(f"\n\nBankruptcy data:\n {bankruptcy_data}")
            #     context["bankruptcy_findings"] = True
            #     context["bankruptcy_data"] = bankruptcy_data
            # else:
            #     context["bankruptcy_findings"] = False
            process_details["populate_sections"]["financial"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["financial"] = str(tb)
        # print("Financial Data:\n", financial_df)
        # print("Bankruptcy Data:\n", bankruptcy_df)

        ############################################################################################################################

        try:
            # Ownership DataFrame
            ownership_data = await populate_ownership(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id,
                                                      session=session)
            state_ownership_df = ownership_data["state_ownership"]
            if not state_ownership_df.empty:
                temp = ownership_data["state_ownership"]
                sown_data = temp.to_dict(orient='records')
                sown_data = remove_time_keys(sown_data)
                context["sown_findings"] = True
                context["sown_data"] = sown_data
            else:
                context["sown_findings"] = False
            process_details["populate_sections"]["gov_ownership"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["gov_ownership"] = str(tb)

    ############################################################################################################################

        # try:
        #     # Country Risk DataFrame
        #     country_risk_data = await populate_country_risk(incoming_ens_id=incoming_ens_id,
        #                                                     incoming_session_id=session_id, session=session)
        #
        #     country_risk_df = country_risk_data["country_risk"]
        #     if not country_risk_df.empty:
        #         temp = country_risk_data["country_risk"]
        #         country_risk_data = temp.to_dict(orient='records')
        #         country_risk_data = remove_time_keys(country_risk_data)
        #         context["country_risk_findings"] = True
        #         context["country_risk_data"] = country_risk_data
        #     else:
        #         context["country_risk_findings"] = False
        #     process_details["populate_sections"]["country_risk"] = "success"
        # except Exception as e:
        #     tb = traceback.format_exc()
        #     process_details["populate_sections"]["country_risk"] = str(tb)

    ############################################################################################################################

        try:
            # Ownership flag DataFrame
            ownership_flag_data = await populate_ownership_flag(incoming_ens_id=incoming_ens_id,
                                                                incoming_session_id=session_id, session=session)
            ownership_flag_df = ownership_flag_data["ownership_flag"]
            if not ownership_flag_df.empty:
                temp = ownership_flag_data["ownership_flag"]
                ownership_flag_data = temp.to_dict(orient='records')
                ownership_flag_data = remove_time_keys(ownership_flag_data)
                context["corporate_ownership_findings"] = True
                context["corporate_ownership_data"] = ownership_flag_data
            else:
                context["corporate_ownership_findings"] = False
            process_details["populate_sections"]["corporate_ownership"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["corporate_ownership"] = str(tb)
        # print("State Ownership Data:\n", state_ownership_df)

        ############################################################################################################################

        try:
            # Other adverse media
            oam = await populate_other_adv_media(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id,
                                                 session=session)
            state_ownership_df = oam["adv_media"]
            if not state_ownership_df.empty:
                temp = oam["adv_media"]
                adv_data = temp.to_dict(orient='records')
                adv_data = remove_time_keys(adv_data)
                context["adv_findings"] = True
                context["adv_data"] = adv_data
            else:
                context["adv_findings"] = False
            process_details["populate_sections"]["adv_media"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["adv_media"] = str(tb)

        ############################################################################################################################

        try:
            # Regulatory and Legal
            ral = await populate_regulatory_legal(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id,
                                                  session=session)
            reg_df = ral["reg_data"]
            leg_df = ral["legal_data"]
            if not reg_df.empty:
                temp = ral["reg_data"]
                regulatory_data = temp.to_dict(orient='records')
                regulatory_data = remove_time_keys(regulatory_data)
                context["reg_findings"] = True
                context["reg_data"] = regulatory_data
            else:
                context["reg_findings"] = False
            if not leg_df.empty:
                temp = ral["legal_data"]
                legal_data = temp.to_dict(orient='records')
                legal_data = remove_time_keys(legal_data)
                context["leg_findings"] = True
                context["leg_data"] = legal_data
            else:
                context["leg_findings"] = False
            process_details["populate_sections"]["regulatory_and_legal"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["regulatory_and_legal"] = str(tb)

        ############################################################################################################################

        try:
            # Cybersecurity DataFrame
            cybersecurity_data = await populate_cybersecurity(incoming_ens_id=incoming_ens_id,
                                                              incoming_session_id=session_id, session=session)
            cybersecurity_df = cybersecurity_data["cybersecurity"]
            if not cybersecurity_df.empty:
                temp = cybersecurity_data["cybersecurity"]
                cyb_data = temp.to_dict(orient='records')
                cyb_data = remove_time_keys(cyb_data)
                context["cyb_findings"] = True
                context["cyb_data"] = cyb_data
            else:
                context["cyb_findings"] = False
            process_details["populate_sections"]["cybersecurity"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["cybersecurity"] = str(tb)
        # print("Cybersecurity Data:\n", cybersecurity_df)

        ############################################################################################################################

        try:
            # ESG DataFrame
            esg_data = await populate_esg(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id,
                                          session=session)
            esg_df = esg_data["esg"]
            if not esg_df.empty:
                temp = esg_data["esg"]
                esgdata = temp.to_dict(orient='records')
                esgdata = remove_time_keys(esgdata)
                context["esg_findings"] = True
                context["esg_data"] = esgdata
            else:
                context["esg_findings"] = False
            process_details["populate_sections"]["esg"] = "success"
        except Exception as e:
            tb = traceback.format_exc()
            process_details["populate_sections"]["esg"] = str(tb)
        # print("ESG Data:\n", esg_df)

        ############################################################################################################################

        # Initialize the document template
        # Fetch `ts_flag` from ts_data
        if ts_data:
            matching_entry = next((entry for entry in ts_data["results"] if entry["ens_id"] == incoming_ens_id), None)
            if matching_entry:
                process_details["L2_supplier_name_validation"] = matching_entry["verification_details"]["is_verified"]
                context['ts_flag'] = matching_entry["verification_details"]["is_verified"]
        else:
            process_details["L2_supplier_name_validation"] = False

        # doc.render(context)
        context["session_id"]=session_id
        context["ens_id"]=incoming_ens_id
        #Call orbis engine endpoint
        print("Retrieving Orbis - Report Generation..")
        # with open(f"123_{context.get("name")}.json", 'w')as file:
        #     json.dump(context,file,indent=2)
        try:
            # Generate JWT token
            jwt_token = create_jwt_token("orchestration", "analysis")
        except Exception as e:
            print("Error generating JWT token:", e)
            raise
        orbis_url = get_settings().urls.orbis_engine
        print("url", orbis_url)
        url = f"{orbis_url}/api/v1/internal/report-generation-node"
        # Prepare headers with the JWT token
        headers = {
            "Authorization": f"Bearer {jwt_token.access_token}"
        }
        response = requests.post(url, headers=headers, json=context)
        print("response of report",response.status_code)
        if response.status_code == 200:
            process_details["blob"]["pdf"] = "success"
            process_details["blob"]["docx"] = "success"
        else:
            process_details["blob"]["pdf"] = "failed"
            process_details["blob"]["docx"] = "failed"


        return process_details,response.status_code

    except Exception as e:
        tb = traceback.format_exc()  # Capture the full traceback

        process_details = {
            "ens_id": incoming_ens_id,
            "supplier_name": incoming_name,
            "L2_supplier_name_validation": "",
            "error": f"ReportGenerationCode - {str(e)}",
            "traceback": tb  # Add detailed traceback info
        }

        logging.error(f"Process details: {process_details}")  # Use logging.error for exceptions

        return process_details, 500