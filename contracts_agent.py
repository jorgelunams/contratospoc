"""
Azure Function Contract Processing Agent

This module provides AI-powered contract analysis using Azure OpenAI.
Only contains the essential function for semantic database extraction from contracts.
"""
import os  
import json  
from semantic_kernel import Kernel  
from semantic_kernel.connectors.ai.open_ai import AzureChatCompletion  
from semantic_kernel.functions import KernelArguments  
from semantic_kernel.contents import ChatHistory


async def extract_semanticdb_from_contract(contract_text_or_json, openai_api_key=None, openai_endpoint=None, model="o1-mini"):
    """
    Analyzes the contract and extracts information in SemanticDB format (Dims/Facts) using Azure OpenAI.
    
    Args:
        contract_text_or_json: Text or JSON of the contract to analyze
        openai_api_key: API key for OpenAI/Azure OpenAI (optional, uses env if not passed)
        openai_endpoint: Endpoint for Azure OpenAI (optional, uses env if not passed)
        model: Model to use (default o1-mini, adjust according to your endpoint)
        
    Returns:
        SemanticDB dictionary extracted or None if it fails
    """
    openai_endpoint = openai_endpoint or os.getenv("OPENAI_API_BASE") or os.getenv("AZURE_OPENAI_ENDPOINT")

    # Build the AI prompt for contract analysis
    prompt = '''Instrucciones:
 
Por favor, analiza el siguiente contrato y extrae la información detallada a continuación. Organiza los datos en un formato JSON estructurado que permita manejar múltiples instancias (por ejemplo, Representante1, Representante2, MultaIncumplimiento1, MultaIncumplimiento2, etc.). Asegúrate de extraer toda la información relevante, prestando especial atención a las multas y sus implicancias.

1. Datos a Extraer:

Tipo de Contrato: (Ejemplo: Anexo, Contrato de Servicios, Confidencialidad (NDA), Carta Término)
Tipo de Servicio: (Ejemplo: Asesoría, Seguridad, Alimentación)
Parte/Contraparte: (Ejemplo: XXX vs YYY)
Fecha de Inicio / Fecha de Término
Renovación Automática: Indica si el contrato se renueva automáticamente.
Monto: Detalle del honorario total y condiciones de pago.
Multas Asociadas: Detalle completo de todas las multas, incluyendo:
Tipo de incumplimiento
Implicancias
Monto de la multa en UF
Plazo para la constancia
Descripción completa
Penalidades: Información sobre otras penalidades aplicables.
¿Término Anticipado?: Especifica el plazo requerido para el preaviso.
¿Exclusividad?: Indica si existe alguna cláusula de exclusividad y proporciona detalles.
Entidades: Extrae todas las entidades relevantes del contrato, como nombres de personas, países y otras entidades importantes.

2. Formato de Salida Esperado:
Organiza la información extraída en un único archivo JSON siguiendo la estructura enumerada que permite múltiples instancias.'''
    
    # Convert contract input to string if it's a JSON object
    if isinstance(contract_text_or_json, dict):
        contract_str = json.dumps(contract_text_or_json, ensure_ascii=False, indent=2)
    else:
        contract_str = str(contract_text_or_json)
    
    prompt += f"\n---\nENTRADA DEL CONTRATO A PROCESAR: **** INICIO CONTRATO ****\n{contract_str}\n---\n"
    prompt += '''*** FIN CONTRATO A PROCESAR ***

INSTRUCCIONES IMPORTANTES:
No inices nunca con ```json, simplemente responde con el JSON completo.
No uses \\n al final de las líneas, el JSON debe estar en una sola línea y limpio puro JSON.
Si no existe el valor pon null o un string vacio "".

### REGLAS ESTRICTAS PARA NOMBRES DE CAMPOS JSON ###
DEBES USAR EXACTAMENTE ESTOS NOMBRES DE CAMPOS - NO LOS CAMBIES NUNCA:

1. SECCIÓN PRINCIPAL: "Contrato" (exactamente así)
2. SECCIÓN DE MULTAS: "Multas" (exactamente así)
3. SECCIÓN DE COMPAÑÍA: "CompaniaInfo" (exactamente así)
4. SECCIÓN DE PROVEEDORES: "ProveedoresInfo" (exactamente así)
5. SECCIÓN DE REPRESENTANTES: "Representantes" (exactamente así)
6. SECCIÓN DE ENTIDADES: "Entidades" (exactamente así)

### ESTRUCTURA OBLIGATORIA ###
Tu JSON DEBE tener exactamente esta estructura principal:
{
  "Contrato": { 
    "tipo_contrato": "",
    "numero_contrato": "",
    "tipo_servicio": "",
    "parte_cliente": "",
    "parte_proveedor": "",
    "fecha_inicio": "",
    "fecha_termino": "",
    "renovacion_automatica": false,
    "monto_total": 0,
    "multa_monto": 0,
    "multa_penalidades": "",
    "termino_anticipado_activo": false,
    "termino_anticipado_plazo_dias": 0,
    "exclusividad_activo": false,
    "exclusividad_detalles": "",
    "descripcion": "",
    "nombre": ""
  },
  "Multas": [],
  "CompaniaInfo": {},
  "ProveedoresInfo": [],
  "Representantes": [],
  "Entidades": []
}

IMPORTANTE: EXTRAE TODAS LAS ENTIDADES DEL CONTRATO, NO TE LIMITES A 5 ENTIDADES.
NO INVENTES NI COPIES DATOS DE EJEMPLO. Solo reporta lo que está en el contrato a procesar.

Asegúrate de que el JSON resultante sea válido y siga EXACTAMENTE la estructura proporcionada.
AQUI TU RESPUESTA DEBE SER UN JSON VALIDO no comiences con ```json ni termines con ```'''

    # Set up Azure OpenAI configuration
    service_id = "reasoning"
    endpoint = os.getenv("AZURE_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    model_name = os.getenv("AZURE_MODEL_NAME")
    
    if not model_name:
        model_name = "o1-mini"  # Default value if model not specified
    
    try:
        # Initialize Azure OpenAI chat service
        chat_service = AzureChatCompletion(
            service_id=service_id,
            endpoint=endpoint,
            api_key=api_key,
            deployment_name=model_name,
            api_version="2024-12-01-preview"
        )
        
        # Create semantic kernel
        kernel = Kernel()
        kernel.add_service(chat_service)
        
        # Set up execution settings
        req_settings = kernel.get_prompt_execution_settings_from_service_id(service_id=service_id)
        req_settings.max_completion_tokens = 35000
        
        # Create function arguments
        arguments = KernelArguments(input=prompt)
        
        # Add chat function to kernel
        chat_function = kernel.add_function(
            prompt=prompt,
            function_name="chat",
            plugin_name="chat",
            prompt_execution_settings=req_settings,
        )
        
        # Initialize chat history
        chat_history = ChatHistory()
        chat_history.add_user_message("Hola, ¿Procesa este contrato?")
        chat_history.add_assistant_message("Expert in Contract Processing.")
        
        # Stream the AI response
        accumulated_messages = ""
        answer = kernel.invoke_stream(
            chat_function,
            arguments=arguments,
        )
        
        # Collect all response messages
        async for message in answer:
            accumulated_messages += str(message[0])
            
        return accumulated_messages
        
    except Exception as e:
        print(f"Error occurred converting contract to DIM/FACT format: {e}")
        return None
