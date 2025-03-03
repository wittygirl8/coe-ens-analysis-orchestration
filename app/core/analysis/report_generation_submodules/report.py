# http://127.0.0.1:8000/#/

import asyncio
import logging
import os
from datetime import datetime
from io import BytesIO
import traceback
from dotenv import load_dotenv
from docxtpl import DocxTemplate
from docx2pdf import convert
from app.core.utils.db_utils import *
from app.core.analysis.report_generation_submodules.utilities import *
import io
import tempfile
import requests 
import json
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

async def report_generation(data, session, upload_to_blob:bool, save_locally:bool, ts_data=None):
    save_locally=True
    incoming_ens_id = data["ens_id"]
    incoming_country = data["country"]
    incoming_name = data["name"]
    session_id = data["session_id"]
    # national_id = data["national_id"]
    
    template = r"app\core\analysis\report_generation_submodules\template_A.docx"
    output_folder = r"app\core\analysis\report_generation_submodules\output"

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
        supplier_data = await get_dynamic_ens_data("upload_supplier_master_data", required_columns=required_columns, ens_id=incoming_ens_id, session_id=session_id, session=session)
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

        # summary = generate_summary(supplier_name=incoming_name)

        context = {}
        sanctions = await sape_summary(data, session)
        bcf = await bcf_summary(data, session)
        sco = await state_ownership_summary(data,session)
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
        print("profile_data", profile_data)
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
                context["corruption_data"] = bribery_data
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
            financials_data = await populate_financials(incoming_ens_id=incoming_ens_id, incoming_session_id=session_id, session=session)
            financial_df = financials_data["financial"]
            bankruptcy_df = financials_data["bankruptcy"]
            if not financial_df.empty:
                temp = financials_data["financial"]
                financial_data = temp.to_dict(orient='records')
                context["financial_findings"] = True
                context["financial_data"] = financial_data
            else:
                context["financial_findings"] = False
            if not bankruptcy_df.empty:
                temp = financials_data["bankruptcy"]
                bankruptcy_data = temp.to_dict(orient='records')
                context["bankruptcy_findings"] = True
                context["bankruptcy_data"] = bankruptcy_data
            else:
                context["bankruptcy_findings"] = False
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

        # # Save the PDF
        # pdf_buffer = BytesIO()
        # convert(docx_buffer, pdf_buffer)
        # pdf_buffer.seek(0)

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
            # for file in os.listdir(output_folder):
            #     file_path = os.path.join(output_folder, file)
            #     if os.path.isfile(file_path):
            #         os.remove(file_path)
            # logging.info(f"Output folder cleared: Awaiting new files...")
            logging.info(f"Output folder present: Awaiting new files...")

        docx_path = os.path.join(output_folder, docx_file_local)
        pdf_path = os.path.join(output_folder, pdf_file_local)

        # Save locally if requested
        if save_locally:
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
                pdf_upload_success = upload_to_azure_blob(pdf_buffer, pdf_file, session_id)
                process_details["blob"]["pdf"] = "success" if pdf_upload_success else "failed"
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
    
