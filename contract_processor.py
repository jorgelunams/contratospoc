"""
Azure Function App for Contract Processing Pipeline
==================================================

This Azure Function implements a complete contract ingestion and processing pipeline.
The workflow handles PDF contracts uploaded to blob storage and processes them through:

1. EventGrid trigger detection of new PDF uploads
2. PDF text extraction using Azure Document Intelligence
3. AI-powered semantic data extraction using Azure OpenAI
4. Data validation and structured model creation
5. SQL database storage with relational integrity

Main Components:
- EventGrid trigger for real-time processing
- Document Intelligence for PDF text extraction
- AI agent for contract semantic analysis
- Data models for structured contract information
- SQL database integration for persistence

Author: Contract Processing Team
Version: 1.0 - Production Ready
Last Updated: July 2025
"""

import logging  
import json  
import traceback  
from urllib.parse import urlparse  
import azure.functions as func  
import os  
import asyncio  
from dotenv import load_dotenv
from datetime import datetime   
from azure.core.credentials import AzureKeyCredential  
from azure.ai.documentintelligence import DocumentIntelligenceClient  
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from datalake_services import generate_sas_url, download_blob 
from contract_models import (
    Contrato, CompaniaInfo, ProveedoresInfo, 
    Representante, Entidades, Multa
)
import datalake_services

# Load environment variables from .env file  
load_dotenv()  
  
app = func.FunctionApp()  
   
async def process_contract_data(data):
    """
    Transform AI-extracted contract data into structured Python dataclass models.
    
    This function serves as the critical data transformation layer between the AI extraction
    and database storage. It handles:
    - Field name normalization (AI outputs vs Python model fields)
    - Data type validation and conversion
    - Creation of structured dataclass instances
    - Error handling for malformed or missing data
    
    The function maps AI-generated field names (which often contain spaces and capitals)
    to Python dataclass field names (lowercase with underscores) to ensure compatibility.
    
    Args:
        data (dict): Raw contract data extracted by AI containing sections:
                    - Contrato: Main contract information
                    - CompaniaInfo: Client company details
                    - ProveedoresInfo: Provider/vendor information
                    - Representantes: List of contract representatives
                    - Multas: List of penalties and fines
                    - Entidades: List of extracted entities
    
    Returns:
        dict: Structured contract data ready for database insertion containing:
             - Validated dataclass instances converted to dictionaries
             - Metadata with processing timestamp
             - Unique contract ID for tracking
             
    Raises:
        ValueError: When required contract sections are missing
        KeyError: When critical fields are not found in the data
        Exception: For unexpected data validation or processing errors
    
    Note:
        This function is essential for data quality - it ensures only valid, 
        properly structured data reaches the database layer.
    """
    try:
        # Field mapping dictionaries to normalize AI-generated field names to Python dataclass fields
        
        # Mapping for Multas fields (AI generates with caps and spaces, models expect lowercase with underscores)
        MULTA_FIELD_MAPPING = {
            'Tipo de incumplimiento': 'tipo_incumplimiento',
            'tipo de incumplimiento': 'tipo_incumplimiento',
            'tipo_incumplimiento': 'tipo_incumplimiento',
            'Implicancias': 'implicancias',
            'implicancias': 'implicancias',
            'Monto de la multa en UF': 'monto_multa_uf',
            'monto de la multa en uf': 'monto_multa_uf',
            'monto_multa_uf': 'monto_multa_uf',
            'Plazo para la constancia': 'plazo_constancia',
            'plazo para la constancia': 'plazo_constancia',
            'plazo_constancia': 'plazo_constancia',
            'Descripción completa': 'descripcion_completa',
            'descripción completa': 'descripcion_completa',
            'descripcion_completa': 'descripcion_completa'
        }
        
        # Mapping for Representantes fields
        REPRESENTANTE_FIELD_MAPPING = {
            'Nombre': 'nombre',
            'nombre': 'nombre',
            'Cédula de Identidad': 'cedula_identidad',
            'cedula de identidad': 'cedula_identidad',
            'cedula_identidad': 'cedula_identidad',
            'Cedula de Identidad': 'cedula_identidad',
            'cedula identidad': 'cedula_identidad'
        }
        
        # Mapping for CompaniaInfo and ProveedoresInfo fields
        COMPANY_FIELD_MAPPING = {
            'Nombre': 'nombre',
            'nombre': 'nombre',
            'RUT': 'rut',
            'rut': 'rut',
            'Domicilio': 'domicilio',
            'domicilio': 'domicilio'
        }        # Mapping for Entidades fields (SQL expects tipo_entidad and nombre, model has tipo and valor)
        ENTIDADES_FIELD_MAPPING = {
            'tipo': 'tipo',
            'Tipo': 'tipo',
            'tipo_entidad': 'tipo',
            'Tipo_entidad': 'tipo',
            'valor': 'valor',
            'Valor': 'valor',
            'nombre': 'valor',  # Map 'nombre' from SQL to 'valor' in model
            'Nombre': 'valor'
        }

        def normalize_multa_fields(multa_dict):
            """
            Normalize multa field names to match Python dataclass.
            
            The AI extraction returns field names with Spanish text, spaces, and capitals
            (e.g., "Tipo de incumplimiento") while our Python models expect snake_case
            field names (e.g., "tipo_incumplimiento"). This function bridges that gap.
            """
            normalized = {}
            logging.info(f"Normalizing multa fields from: {list(multa_dict.keys())}")
            for key, value in multa_dict.items():
                normalized_key = MULTA_FIELD_MAPPING.get(key, key.lower().replace(' ', '_'))
                normalized[normalized_key] = value
                if key != normalized_key:
                    logging.info(f"Mapped multa field: '{key}' -> '{normalized_key}'")
            logging.info(f"Normalized multa fields to: {list(normalized.keys())}")
            return normalized

        def normalize_representante_fields(rep_dict):
            """
            Normalize representante field names to match Python dataclass.
            
            Maps Spanish field names from AI extraction to Python model field names.
            Handles variations in capitalization and spacing.
            """
            normalized = {}
            logging.info(f"Normalizing representante fields from: {list(rep_dict.keys())}")
            for key, value in rep_dict.items():
                normalized_key = REPRESENTANTE_FIELD_MAPPING.get(key, key.lower().replace(' ', '_'))
                normalized[normalized_key] = value
                if key != normalized_key:
                    logging.info(f"Mapped representante field: '{key}' -> '{normalized_key}'")
            return normalized

        def normalize_company_fields(company_dict):
            """
            Normalize company field names to match Python dataclass.
            
            Used for both CompaniaInfo and ProveedoresInfo sections since they
            share the same field structure (nombre, rut, domicilio).
            """
            normalized = {}
            logging.info(f"Normalizing company fields from: {list(company_dict.keys())}")
            for key, value in company_dict.items():
                normalized_key = COMPANY_FIELD_MAPPING.get(key, key.lower().replace(' ', '_'))
                normalized[normalized_key] = value
                if key != normalized_key:
                    logging.info(f"Mapped company field: '{key}' -> '{normalized_key}'")
            return normalized

        def normalize_entidades_fields(entidad_dict):
            """
            Normalize entidades field names to match Python dataclass.
            
            Maps between AI extraction format and SQL storage format:
            - AI uses 'tipo' and 'valor' 
            - SQL may use 'tipo_entidad' and 'nombre'
            This function normalizes to the model's expected 'tipo' and 'valor' fields.
            """
            normalized = {}
            logging.info(f"Normalizing entidades fields from: {list(entidad_dict.keys())}")
            for key, value in entidad_dict.items():
                normalized_key = ENTIDADES_FIELD_MAPPING.get(key, key.lower())
                normalized[normalized_key] = value
                if key != normalized_key:
                    logging.info(f"Mapped entidades field: '{key}' -> '{normalized_key}'")
            return normalized

        # Helper function to safely get data from dict or list
        def safe_get_data(source_data, key, default=None):
            """
            Safely extract data from AI response which may be in various formats.
            
            The AI sometimes returns data as nested objects or arrays, so this function
            provides a robust way to extract values regardless of the structure.
            """
            if isinstance(source_data, dict):
                return source_data.get(key, default)
            elif isinstance(source_data, list):
                # If it's a list, look for the key in each item
                for item in source_data:
                    if isinstance(item, dict) and key in item:
                        return item[key]
                return default
            else:
                logging.warning(f"Unexpected data type for {key}: {type(source_data)}")
                return default

        # Helper function to ensure we get a dict from the result
        def ensure_dict(data_item, field_name):
            """
            Ensure we get a dictionary from AI response data.
            
            Sometimes the AI returns a list when we expect a single object,
            or other unexpected formats. This function normalizes the structure.
            """
            if isinstance(data_item, dict):
                return data_item
            elif isinstance(data_item, list):
                if len(data_item) > 0 and isinstance(data_item[0], dict):
                    logging.info(f"{field_name} was a list, using first item as dict")
                    return data_item[0]
                else:
                    logging.error(f"{field_name} is a list but doesn't contain dict items")
                    return {}
            else:
                logging.error(f"{field_name} is neither dict nor list: {type(data_item)}")
                return {}        # Create Contrato instance
        contrato_data = safe_get_data(data, 'Contrato', {})
        contrato_data = ensure_dict(contrato_data, 'Contrato')
        if not contrato_data:
            raise ValueError("Missing Contrato data")
        contrato = Contrato.from_dict(contrato_data)
        
        # Create CompaniaInfo instance
        compania_data = safe_get_data(data, 'CompaniaInfo', {})
        compania_data = ensure_dict(compania_data, 'CompaniaInfo')
        if not compania_data:
            raise ValueError("Missing CompaniaInfo data")
        # Normalize field names for CompaniaInfo
        compania_data = normalize_company_fields(compania_data)
        compania_info = CompaniaInfo.from_dict(compania_data)
        
        # Create ProveedoresInfo instance
        proveedor_data = safe_get_data(data, 'ProveedoresInfo', {})
        proveedor_data = ensure_dict(proveedor_data, 'ProveedoresInfo')
        logging.info(f"ProveedoresInfo data type after ensure_dict: {type(proveedor_data)}")
        if not proveedor_data:
            raise ValueError("Missing ProveedoresInfo data")
        # Normalize field names for ProveedoresInfo
        proveedor_data = normalize_company_fields(proveedor_data)
        proveedor_info = ProveedoresInfo.from_dict(proveedor_data)
        
        # Process Representantes
        representantes_data = safe_get_data(data, 'Representantes', [])
        representantes = []
        for rep_data in representantes_data:
            if isinstance(rep_data, dict):
                # Normalize field names for each Representante
                normalized_rep_data = normalize_representante_fields(rep_data)
                representantes.append(Representante.from_dict(normalized_rep_data))
    
        # Process Multas
        multas_data = safe_get_data(data, 'Multas', [])
        multas = []
        for multa_data in multas_data:
            if isinstance(multa_data, dict):
                # Normalize field names for each Multa
                normalized_multa_data = normalize_multa_fields(multa_data)
                multas.append(Multa.from_dict(normalized_multa_data))
                  # Process Entidades (expected as an array from AI)
        entidades_data = safe_get_data(data, 'Entidades', [])
        entidades_list = []
        
        # Handle both array and single object cases
        if isinstance(entidades_data, list):
            for entidad_data in entidades_data:
                if isinstance(entidad_data, dict):
                    # Normalize field names for each Entidad
                    normalized_entidad_data = normalize_entidades_fields(entidad_data)
                    entidades_list.append(Entidades.from_dict(normalized_entidad_data))
        elif isinstance(entidades_data, dict):
            # Handle single object case (fallback)
            normalized_entidad_data = normalize_entidades_fields(entidades_data)
            entidades_list.append(Entidades.from_dict(normalized_entidad_data))
        else:
            logging.warning(f"Unexpected Entidades data type: {type(entidades_data)}")
        
        # For backward compatibility, if we have entidades, use the first one as the main entidades object
        entidades = entidades_list[0] if entidades_list else Entidades(tipo="", valor="")
          # Create structured data for database
        structured_contract_data = {
            "id": f"contrato_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "Contrato": vars(contrato),
            "CompaniaInfo": vars(compania_info),
            "ProveedoresInfo": vars(proveedor_info),
            "Representantes": [vars(rep) for rep in representantes],
            "Multas": [vars(multa) for multa in multas],
            "Entidades": vars(entidades),  # Backward compatibility - single object
            "EntidadesList": [vars(entidad) for entidad in entidades_list],  # Full list for database
            "metadata": {
                "processedDate": datetime.now().isoformat()
            }
        }
        
        # Log success
        logging.info("Successfully created contract model instances")
        logging.info(f"Found {len(representantes)} representantes")
        logging.info(f"Found {len(multas)} multas")
        logging.info(f"Found {len(entidades_list)} entidades")
        logging.info("Database objects created for all required models")
        
        return structured_contract_data
        
    except ValueError as ve:
        logging.error(f"Data validation error: {str(ve)}")
        raise
    except KeyError as ke:
        logging.error(f"Missing required field in contract data: {ke}")
        raise
    except Exception as e:
        logging.error(f"Error processing contract data: {str(e)}")
        logging.error(traceback.format_exc())
        raise
  
@app.function_name(name="EventGridContratos")  
@app.event_grid_trigger(arg_name="event")  
async def EventGridContratos(event: func.EventGridEvent):
    """
    Azure Function entry point for EventGrid-triggered contract processing.
    
    This is the main Azure Function that responds to EventGrid events when new files
    are uploaded to the blob storage container. It serves as a lightweight wrapper
    that delegates the actual processing to the core business logic function.
    
    The function is decorated with Azure Function triggers and acts as the 
    cloud-native entry point for the contract processing pipeline.
    
    Args:
        event (func.EventGridEvent): EventGrid event containing blob upload details
                                   including storage account, container, and file path
    
    Returns:
        dict: Processing result status from the core business logic
        
    Note:
        This function should remain minimal - all business logic is handled
        by process_event_grid_contratos_core() for better testability.
    """
    # Delegate to the core business logic function
    return await process_event_grid_contratos_core(event)

async def process_pdf(storage_account_name, storage_account_key, container_name, folder_name, file_name):  
    """
    Extract structured text content from PDF files using Azure Document Intelligence.
    
    This function handles the PDF-to-text conversion phase of the contract processing
    pipeline. It uses Azure's Document Intelligence service (formerly Form Recognizer)
    to perform advanced OCR and layout analysis on PDF documents.
    
    The function performs these key operations:
    1. Generates a SAS URL for secure blob access
    2. Calls Azure Document Intelligence API with the PDF URL
    3. Processes the response to extract text by page
    4. Structures the text data into a page-organized JSON format
    5. Uploads the resulting JSON back to blob storage for downstream processing
    
    Args:
        storage_account_name (str): Azure storage account containing the PDF
        storage_account_key (str): Access key for the storage account
        container_name (str): Blob container name where PDF is stored
        folder_name (str): Folder path within container (can be None)
        file_name (str): Name of the PDF file to process
    
    Returns:
        str: JSON string containing structured text data organized by pages,
             or error JSON if processing fails
             
    JSON Structure:
        {
            "Page-1": ["line1 text", "line2 text", ...],
            "Page-2": ["line1 text", "line2 text", ...],
            ...
        }
    
    Note:
        The function uses the 'prebuilt-layout' model which provides both text
        extraction and layout understanding, preserving document structure.
        Error handling ensures graceful degradation if document processing fails.
    """
    endpoint = os.getenv("DOCUMENT_INTELLIGENCE_ENDPOINT")
    key = os.getenv("DOCUMENT_INTELLIGENCE_KEY")
    if not endpoint or not key:
        logging.error("Document Intelligence endpoint or key is not set in environment variables.")
        error_msg = "Document Intelligence endpoint or key is not set"
        return json.dumps({"error": error_msg})

    try:
        # Build the blob path for the input PDF
        if folder_name:
            blob_path = f"{folder_name}/{file_name}"
        else:
            blob_path = file_name

        # Get SAS URL from datalake_services
        formUrl = generate_sas_url(storage_account_name, storage_account_key, container_name, blob_path)
        if not formUrl:
            error_msg = f"Failed to generate SAS URL for {blob_path}"
            logging.error(error_msg)
            return json.dumps({"error": error_msg})

        document_intelligence_client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))  
        # Analyze the document using the correct method for URL input
        poller = document_intelligence_client.begin_analyze_document(
            "prebuilt-layout",
            AnalyzeDocumentRequest(url_source=formUrl)
        )
        result = poller.result()

        # Prepare the structured data  
        structured_data = {}  
        for page_number, page in enumerate(result.pages, start=1):  
            page_key = f"Page-{page_number}"  
            structured_data[page_key] = [line.content for line in page.lines]  
        
        # Convert to JSON  
        json_data = json.dumps(structured_data, ensure_ascii=False)  
        
        # Save JSON data to Azure Data Lake
        if file_name.lower().endswith('.pdf'):
            output_file_name = file_name[:-4] + '.json'
        else:
            output_file_name = file_name + '.json'
        stream = json_data.encode('utf-8')
        
        # Wrap the upload in try/except to catch any errors
        try:
            await datalake_services.upload_file_stream(storage_account_name, storage_account_key, container_name, folder_name or "", output_file_name, stream)
        except Exception as upload_error:
            logging.error(f"Error uploading JSON to data lake: {str(upload_error)}")
            # Continue with the function even if upload fails
            # We'll return the JSON data anyway
        
        return json_data
        
    except Exception as e:
        logging.error(f"Error in process_pdf: {str(e)}")
        logging.error(traceback.format_exc())
        # Return error JSON instead of raising to avoid None return
        return json.dumps({"error": str(e)})

# Core business logic function without Azure Functions decorators
async def process_event_grid_contratos_core(event):
    """
    Core orchestrator function for the complete contract processing pipeline.
    
    This is the heart of the contract processing system, orchestrating the entire
    workflow from PDF upload detection through final database storage. The function
    is designed to be cloud-agnostic and testable outside of Azure Functions runtime.
    
    Complete Processing Pipeline:
    1. Event Validation & Deduplication
       - Validates EventGrid event structure
       - Checks for duplicate processing to prevent loops
       - Filters for PDF files only
    
    2. PDF Text Extraction
       - Uses Azure Document Intelligence for OCR
       - Converts PDF to structured JSON text data
       - Handles multi-page documents with layout preservation
    
    3. AI-Powered Semantic Extraction
       - Processes extracted text through Azure OpenAI
       - Extracts structured contract data (companies, dates, terms, penalties)
       - Returns standardized JSON with contract semantics
    
    4. Data Validation & Model Creation
       - Transforms AI output into validated Python dataclass models
       - Normalizes field names and validates data types
       - Ensures data integrity before database insertion
    
    5. Database Persistence
       - Stores structured contract data in SQL database
       - Maintains relational integrity across contract entities
       - Returns unique contract ID for tracking
    
    Args:
        event (func.EventGridEvent): EventGrid event containing:
                                   - Blob storage details (account, container, path)
                                   - Event metadata (ID, type, timestamp)
                                   - File information for processing
    
    Returns:
        dict: Processing result containing one of:
              - {"status": "success", "event_id": str, "contract_id": str}
              - {"status": "skipped", "reason": str, "event_id": str}  
              - {"status": "error", "reason": str, "event_id": str}
    
    Error Handling:
        The function implements comprehensive error handling at each stage:
        - Network failures during blob access
        - Document Intelligence service errors
        - AI extraction failures or malformed responses
        - Database connection or insertion errors
        - Data validation and transformation errors
    
    Note:
        This function is designed to be idempotent - it can safely be retried
        without causing duplicate processing or data corruption.
    """
    event_id = event.id

    try:
        # Extract event data and validate
        event_data = event.get_json()
        subject = event.subject
        event_type = event.event_type
        data = event_data

        # Log event details
        logging.info(f"Event ID: {event_id}")
        logging.info(f"Event Type: {event_type}")
        logging.info(f"Subject: {subject}")
        logging.info(f"Data: {json.dumps(data)}")

        # Skip processing for processed events
        if "/containers/processed-events/" in subject.lower():
            logging.info(f"Event {event_id} is from 'processed-events' container. Skipping to prevent loop.")
            return {"status": "skipped", "reason": "processed-events container", "event_id": event_id}

        # Check if already processed
        has_been_processed = await datalake_services.has_event_been_processed(event_id)
        if has_been_processed:
            logging.info(f"Event {event_id} has already been processed. Skipping.")
            return {"status": "skipped", "reason": "already processed", "event_id": event_id}

        # Handle blob created events
        if event_type != "Microsoft.Storage.BlobCreated":
            logging.info(f"Skipping non-blob event type: {event_type}")
            return {"status": "skipped", "reason": "not a blob event", "event_id": event_id}

        # Extract blob info
        url = data.get('url')
        if not url:
            return {"status": "error", "reason": "missing url", "event_id": event_id}

        # Parse blob URL
        parsed_url = urlparse(url)
        storage_account = parsed_url.netloc.split('.')[0]
        path_parts = parsed_url.path.strip('/').split('/')
        container_name = path_parts[0] if path_parts else None
        blob_path = '/'.join(path_parts[1:]) if len(path_parts) > 1 else None

        if not blob_path:
            return {"status": "error", "reason": "missing blob path", "event_id": event_id}

        # Get folder and file names
        blob_parts = blob_path.split('/')
        if len(blob_parts) >= 2:
            folder_name = '/'.join(blob_parts[:-1])
            file_name = blob_parts[-1]
        else:
            folder_name = None
            file_name = blob_parts[0] if blob_parts else None

        if not file_name:
            return {"status": "error", "reason": "missing file name", "event_id": event_id}

        if "pdf" not in file_name.lower():
            return {"status": "skipped", "reason": "not a pdf file", "event_id": event_id}
            
        # Mark as processed
        await datalake_services.mark_event_as_processed(event_id)
        logging.info(f"Event marked as processed: {event_id}")
        
        # Get lake key
        lake_key = os.getenv("LAKE_KEY")
        if not lake_key:
            return {"status": "error", "reason": "missing lake key", "event_id": event_id}
            
        # Process PDF
        logging.info(f"Processing PDF file: {file_name}")
        processed_pdf = await process_pdf(storage_account, lake_key, container_name, folder_name, file_name)
        if not processed_pdf:
            logging.error("process_pdf returned None or empty result")
            return {"status": "error", "reason": "pdf processing failed", "event_id": event_id}

        # Generate JSON filename
        json_file_name = file_name[:-4] + '.json' if file_name.lower().endswith('.pdf') else file_name + '.json'

        # Load and process JSON content
        from contracts_agent import extract_semanticdb_from_contract
        storageaccount = "walmartchiledatalake"
        stream = await download_blob(lake_key, container_name, storageaccount, folder_name, json_file_name)
        
        if not stream:
            return {"status": "error", "reason": "failed to load json", "event_id": event_id}
            
        try:
            json_content = json.load(stream)
            logging.info("Successfully loaded JSON content from blob")
              # Extract semantic data
            logging.info("Calling extract_semanticdb_from_contract")
            semanticdb_result = await extract_semanticdb_from_contract(json_content)
            
            if semanticdb_result is None:
                logging.error("extract_semanticdb_from_contract returned None")
                return {"status": "error", "reason": "semantic extraction failed", "event_id": event_id}
              # Parse the semantic data
            try:
                if isinstance(semanticdb_result, str):
                    # Add debugging for JSON parsing issues
                    try:
                        contract_data = json.loads(semanticdb_result)
                    except json.JSONDecodeError as json_err:
                        logging.error(f"JSON parse error at position {json_err.pos}. Content around error:")
                        start_pos = max(0, json_err.pos - 50)
                        end_pos = min(len(semanticdb_result), json_err.pos + 50)
                        error_context = semanticdb_result[start_pos:end_pos]
                        logging.error(f"...{error_context}...")
                        return {"status": "error", "reason": f"json_decode_error: {str(json_err)}", "event_id": event_id}
                else:
                    contract_data = semanticdb_result
                logging.info("Successfully parsed semanticdb_result")
                
                # Debug: Log the type and structure of contract_data
                logging.info(f"contract_data type: {type(contract_data)}")
                if isinstance(contract_data, list):
                    logging.info(f"contract_data is a list with {len(contract_data)} items")
                    if len(contract_data) > 0:
                        logging.info(f"First item type: {type(contract_data[0])}")
                        # If it's a list, take the first item if it exists and is a dict
                        if isinstance(contract_data[0], dict):
                            contract_data = contract_data[0]
                            logging.info("Using first item from list as contract_data")
                        else:
                            logging.error("First item in list is not a dictionary")
                            return {"status": "error", "reason": "invalid_contract_data_structure", "event_id": event_id}
                    else:
                        logging.error("contract_data is an empty list")
                        return {"status": "error", "reason": "empty_contract_data_list", "event_id": event_id}
                elif isinstance(contract_data, dict):
                    logging.info("contract_data is a dictionary")
                else:
                    logging.error(f"contract_data is neither list nor dict: {type(contract_data)}")
                    return {"status": "error", "reason": "unexpected_contract_data_type", "event_id": event_id}
                    
            except json.JSONDecodeError as je:
                error_pos = je.pos
                start = max(0, error_pos - 50)
                end = min(len(semanticdb_result), error_pos + 50)
                logging.error(f"JSON parse error at position {error_pos}. Content around error:")
                logging.error(f"...{semanticdb_result[start:error_pos]}<<e>>{semanticdb_result[error_pos:end]}...")
                return {"status": "error", "reason": f"json_decode_error: {str(je)}", "event_id": event_id}
            
            if not contract_data:
                logging.error("No contract data to process")
                return {"status": "error", "reason": "no_contract_data", "event_id": event_id}
            
            # Additional validation: Ensure contract_data is a dictionary before processing
            if not isinstance(contract_data, dict):
                logging.error(f"contract_data is not a dictionary after processing. Type: {type(contract_data)}")
                logging.error(f"contract_data content: {str(contract_data)[:500]}...")  # Log first 500 chars
                return {"status": "error", "reason": "contract_data_not_dict_after_processing", "event_id": event_id}
            
            # Debug: Log the keys in contract_data to understand structure
            logging.info(f"contract_data keys: {list(contract_data.keys())}")
            
            # Process contract data into our models
            structured_data = await process_contract_data(contract_data)
            if not structured_data:
                logging.error("process_contract_data returned None or empty result")
                return {"status": "error", "reason": "contract processing failed", "event_id": event_id}
            
            logging.info("Successfully processed contract data into models")            # Store in SQL database
            from contratosdb import insert_contract_data
            try:
                # The insert_contract_data function handles its own connection
                contract_id = insert_contract_data(structured_data)
                if not contract_id:
                    raise ValueError("Failed to insert contract data")
                
                logging.info(f"Successfully inserted contract data with ID: {contract_id}")
                
                logging.info(f"Finished processing event {event_id} at {datetime.now()}")
                return {"status": "success", "event_id": event_id, "contract_id": contract_id}
                
            except Exception as db_error:
                logging.error(f"Error inserting data into SQL database: {str(db_error)}")
                return {"status": "error", "reason": f"database_error: {str(db_error)}", "event_id": event_id}
        
        except Exception as e:
            logging.error(f"Error processing contract: {str(e)}")
            logging.error(traceback.format_exc())
            return {"status": "error", "reason": str(e), "event_id": event_id}

    except Exception as e:
        logging.error(f"Unexpected error processing event: {str(e)}")
        logging.error(traceback.format_exc())
        return {"status": "error", "error": str(e), "event_id": event_id}

if __name__ == "__main__":
    # Configure logging for debug session
    logging.basicConfig(level=logging.INFO)
    
    print("Starting debug session for Azure Function Contract Processor...")
    print("=" * 70)
    
    # Create a mock EventGrid event for testing the complete contract pipeline
    # This simulates what would happen when a PDF is uploaded to blob storage
    mock_event = func.EventGridEvent(
        id=f"debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        data={
            "api": "PutBlob",
            "url": "https://walmartchiledatalake.blob.core.windows.net/contracts/test/CLIMAXX_CPS.pdf",
        },
        event_type="Microsoft.Storage.BlobCreated",
        subject="/blobServices/default/containers/contracts/test/CLIMAXX_CPS.pdf",
        data_version="",
        event_time=datetime.now(),
        topic=""
    )
    
    # Main debug execution - validates the complete processing pipeline
    try:
        # Environment validation - ensure all required services are configured
        print("Validating environment configuration...")
        lake_key = os.getenv("LAKE_KEY")
        print(f"✓ LAKE_KEY configured: {lake_key is not None}")
        
        di_endpoint = os.getenv("DOCUMENT_INTELLIGENCE_ENDPOINT")
        print(f"✓ DOCUMENT_INTELLIGENCE_ENDPOINT configured: {di_endpoint is not None}")
        
        di_key = os.getenv("DOCUMENT_INTELLIGENCE_KEY")
        print(f"✓ DOCUMENT_INTELLIGENCE_KEY configured: {di_key is not None}")
        print()
          
        # Validate function signatures for async compatibility
        print("Validating function compatibility...")
        print(f"✓ EventGridContratos is async: {asyncio.iscoroutinefunction(EventGridContratos)}")
        print(f"✓ process_event_grid_contratos_core is async: {asyncio.iscoroutinefunction(process_event_grid_contratos_core)}")
        print()
        
        # Execute the core processing function that handles the complete pipeline
        print("Executing contract processing pipeline...")
        print("Processing stages: PDF → Text Extraction → AI Analysis → Database Storage")
        print("-" * 70)
        result = asyncio.run(process_event_grid_contratos_core(mock_event))
        print("-" * 70)
        print("Pipeline execution completed!")
        print(f"Final result: {result}")
        
    except KeyboardInterrupt:
        print("\n⚠️  Debug session interrupted by user")
    except Exception as e:
        print(f"\n❌ Error during pipeline execution: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        print("\nFull traceback:")
        traceback.print_exc()
