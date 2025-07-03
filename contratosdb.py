import logging
import pyodbc
from datetime import datetime

logger = logging.getLogger('contratosdb')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_and_normalize_decimal(value_str, max_value=9999999999999999.99, default_value=0.0):
    """
    Validates and normalizes a string to a valid decimal value.
    
    Args:
        value_str: The string value to convert
        max_value: Maximum allowed value
        default_value: Value to return if conversion fails
        
    Returns:
        A float value suitable for SQL DECIMAL column
    """
    if not value_str:
        return default_value
        
    try:
        # Convert to string and normalize
        str_val = str(value_str).replace(',', '.').strip()
        # Remove any non-numeric characters except decimal point
        str_val = ''.join(c for c in str_val if c.isdigit() or c == '.')
        
        # Convert to float
        value = float(str_val) if str_val else default_value
        
        # Check limits
        if value > max_value:
            logger.warning(f"Value {value} exceeds maximum {max_value}, capping")
            return max_value
            
        if value < 0:
            logger.warning(f"Negative value {value} not allowed, using absolute value")
            return abs(value)
            
        return value
        
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid decimal value '{value_str}', using default {default_value}. Error: {str(e)}")
        return default_value

def validate_and_normalize_date(date_str):
    """Validate and normalize date string to SQL format."""
    if not date_str:
        return None
    try:
        # Spanish month names mapping
        spanish_months = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
        }
        
        # Handle Spanish date format like "1 de septiembre de 2024"
        date_str_lower = date_str.lower().strip()
        if ' de ' in date_str_lower:
            parts = date_str_lower.split(' de ')
            if len(parts) == 3:
                day = parts[0].strip()
                month_name = parts[1].strip()
                year = parts[2].strip()
                
                if month_name in spanish_months:
                    # Convert to YYYY-MM-DD format
                    formatted_date = f"{year}-{spanish_months[month_name]}-{day.zfill(2)}"
                    # Validate the date by parsing it
                    datetime.strptime(formatted_date, '%Y-%m-%d')
                    return formatted_date
        
        # Try parsing the date string in common formats
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        raise ValueError(f"Invalid date format: {date_str}")
    except (TypeError, ValueError) as e:
        logging.warning(f"Date validation error: {e}")
        return None

def get_default_cedula(nombre, rep_key):
    """Generate a default cedula value based on name and role."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    nombre_part = ''.join(part for part in nombre.split()[:2])[:10]
    return f"DEFAULT-{nombre_part}-{rep_key}-{timestamp}"

def insert_representatives(cursor, contract_data, contrato_id):
    """Insert representative records with proper error handling."""
    representante_ids = []
    representatives = contract_data.get("Representantes", [])
    
    if not representatives or not isinstance(representatives, list):
        logger.warning("No representatives array found in contract data")
        return representante_ids

    for rep in representatives:
        try:
            if not isinstance(rep, dict):
                logger.warning(f"Skipping invalid representative data format: {rep}")
                continue
                
            nombre = rep.get("Nombre", "").strip()
            cedula = rep.get("Cédula de Identidad", "").strip()
            
            if not nombre:
                logger.warning("Skipping representative with missing name")
                continue

            if not cedula:
                logger.warning(f"Skipping representative {nombre} with missing Cédula de Identidad")
                continue            # Check if this representative already exists in this contract
            cursor.execute("""
                SELECT representante_id FROM Representantes 
                WHERE contrato_id = ? AND cedula_de_identidad = ? AND is_active = 1
            """, (contrato_id, cedula))
            
            existing_rep = cursor.fetchone()
            if existing_rep:
                rep_id = existing_rep[0]
                logger.info(f"Representative {nombre} already exists with ID {rep_id}")
                representante_ids.append(rep_id)
                continue            # Insert the representative
            cursor.execute("""                INSERT INTO Representantes (contrato_id, nombre, cedula_de_identidad, created_at, updated_at)
                VALUES (?, ?, ?, GETDATE(), GETDATE())
            """, (contrato_id, nombre, cedula))
            
            rep_id = cursor.execute("SELECT @@IDENTITY").fetchval()
            logger.info(f"Successfully inserted representante {nombre} with ID {rep_id}")
            representante_ids.append(rep_id)
            
        except Exception as e:
            logger.error("Error inserting representative")
            logger.error(f"Error details: {str(e)}")
            if 'nombre' in locals() and 'cedula' in locals():
                logger.error(f"Data: contrato_id={contrato_id}, nombre='{nombre}', cedula='{cedula}'")
            continue
    
    return representante_ids

def get_available_driver():
    """Get an available SQL Server driver."""
    try:
        drivers = pyodbc.drivers()
        logger.info(f"Available ODBC drivers: {drivers}")
        
        # Try drivers in order of preference
        preferred_drivers = [
            'ODBC Driver 18 for SQL Server',
            'ODBC Driver 17 for SQL Server',
            'SQL Server Native Client 11.0',
            'SQL Server'
        ]
        
        for driver in preferred_drivers:
            if driver in drivers:
                logger.info(f"Using driver: {driver}")
                return driver
                
        if drivers:
            # If none of our preferred drivers are found but there are other drivers
            logger.warning(f"Using non-preferred driver: {drivers[0]}")
            return drivers[0]
            
        raise RuntimeError("No SQL Server ODBC drivers found")
    except Exception as e:
        logger.error(f"Error checking for ODBC drivers: {str(e)}")
        raise

def get_db_connection():
    """Get a connection to Azure SQL Database using SQL authentication."""
    try:
        # Get available driver
        driver = get_available_driver()
        
        # Build the connection string
        connection_parts = [
            f"Driver={{{driver}}}",
            "Server=tcp:walmartdbserver.database.windows.net,1433",
            "Database=walmartdb",
            "UID=adminwalmart",
            "PWD=Walmartsecreto#",
            "Encrypt=Yes",
            "TrustServerCertificate=No",
            "Connection Timeout=30"
        ]
        connection_str = ";".join(connection_parts)
        
        # Create connection
        logger.info(f"Attempting to connect to Azure SQL Database using {driver}...")
        conn = pyodbc.connect(connection_str)
        logger.info("Successfully connected to Azure SQL Database")
        return conn

    except pyodbc.Error as e:
        error_msg = str(e)
        if "IM002" in error_msg:
            logger.error("ODBC Driver not found. Available drivers are:")
            for driver in pyodbc.drivers():
                logger.error(f"  - {driver}")
            logger.error("\nPlease install the Microsoft ODBC Driver for SQL Server:")
            logger.error("Download from: https://go.microsoft.com/fwlink/?linkid=2249006")
            raise
        elif "28000" in error_msg:
            logger.error("Login failed. Please check your username and password.")
            logger.error("\nPlease ensure:")
            logger.error("1. The server exists at walmartdbserver.database.windows.net")
            logger.error("2. The database 'walmartdb' exists")
            logger.error("3. You're using the correct admin credentials")
            logger.error("4. Your IP is allowed in the Azure SQL firewall rules")
            raise
        elif "08001" in error_msg:
            logger.error("\n".join([
                "Unable to connect to Azure SQL Database. Please check:",
                "1. Server name is correct (walmartdbserver.database.windows.net)",
                "2. Your IP is allowed in Azure SQL Database firewall rules",
                "3. You have network connectivity",
                "4. The database exists and is running"
            ]))
            raise
        else:
            logger.error(f"Database connection error: {error_msg}")
            logger.error("\nPlease check that:")
            logger.error("1. Your connection string is correct")
            logger.error("2. The server firewall rules allow your IP")
            logger.error("3. You have network connectivity")
            logger.error("4. The database exists and is running")
            raise

def create_tables(conn):
    """Create all required tables using raw SQL."""
    try:
        cursor = conn.cursor()
        
        # First, drop all foreign key constraints
        logger.info("Dropping all foreign key constraints...")
        if not drop_all_foreign_keys(conn):
            logger.warning("Failed to drop all foreign key constraints, but continuing...")
        
        # Drop tables in the correct order
        tables_to_drop = [
            'Multas',
            'Entidades',
            'Representantes',
            'ProveedoresInfo',
            'CompaniaInfo',
            'Contrato'
        ]
        
        for table_name in tables_to_drop:
            try:
                if verify_table_exists(cursor, table_name):
                    drop_command = f"DROP TABLE {table_name}"
                    cursor.execute(drop_command)
                    conn.commit()
                    logger.info(f"Successfully dropped table: {table_name}")
            except Exception as drop_error:
                logger.error(f"Error dropping table {table_name}: {str(drop_error)}")
                continue

        # Create tables with error handling for each operation
        table_commands = [
            # Contrato table
            """
            CREATE TABLE Contrato (
                id INT IDENTITY(1,1) PRIMARY KEY,
                tipo_contrato VARCHAR(255),
                tipo_servicio VARCHAR(255),
                parte_cliente VARCHAR(255),
                parte_proveedor VARCHAR(255),
                fecha_inicio DATE,
                fecha_termino DATE,
                renovacion_automatica BIT,
                monto_total DECIMAL(18, 2),
                multa_monto DECIMAL(18, 2),
                multa_penalidades VARCHAR(MAX),
                termino_anticipado_activo BIT,
                termino_anticipado_plazo_dias INT,
                exclusividad_activo BIT,
                exclusividad_detalles VARCHAR(MAX),
                descripcion VARCHAR(MAX),
                nombre VARCHAR(255),
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE(),
                is_active BIT DEFAULT 1
            );
            """,
            
            # Multas table
            """
            CREATE TABLE Multas (
                id INT IDENTITY(1,1) PRIMARY KEY,
                contrato_id INT NOT NULL,
                tipo_incumplimiento VARCHAR(MAX) NOT NULL,
                implicancias VARCHAR(MAX),
                monto_multa_uf VARCHAR(50),
                plazo_constancia VARCHAR(255),
                descripcion_completa VARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE(),
                is_active BIT DEFAULT 1,
                CONSTRAINT FK_multas_contrato FOREIGN KEY (contrato_id) REFERENCES Contrato(id)
            );
            """,
            
            # CompaniaInfo table
            """
            CREATE TABLE CompaniaInfo (
                id INT IDENTITY(1,1) PRIMARY KEY,
                contrato_id INT NOT NULL,
                nombre VARCHAR(255),
                rut VARCHAR(20),
                domicilio VARCHAR(255),
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE(),
                is_active BIT DEFAULT 1,
                CONSTRAINT FK_compania_contrato FOREIGN KEY (contrato_id) REFERENCES Contrato(id)
            );
            """,
            
            # ProveedoresInfo table
            """
            CREATE TABLE ProveedoresInfo (
                id INT IDENTITY(1,1) PRIMARY KEY,
                contrato_id INT NOT NULL,
                nombre VARCHAR(255),
                rut VARCHAR(20),
                domicilio VARCHAR(255),
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE(),
                is_active BIT DEFAULT 1,
                CONSTRAINT FK_proveedores_contrato FOREIGN KEY (contrato_id) REFERENCES Contrato(id)
            );
            """,
            
            # Representantes table
            """
            CREATE TABLE Representantes (
                id INT IDENTITY(1,1) PRIMARY KEY,
                contrato_id INT NOT NULL,
                nombre VARCHAR(255) NOT NULL,
                cedula_de_identidad VARCHAR(50) NOT NULL,
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE(),
                is_active BIT DEFAULT 1,
                CONSTRAINT FK_representantes_contrato FOREIGN KEY (contrato_id) REFERENCES Contrato(id)
            );
            """,
            
            # Entidades table
            """
            CREATE TABLE Entidades (
                id INT IDENTITY(1,1) PRIMARY KEY,
                contrato_id INT NOT NULL,
                tipo VARCHAR(255) NOT NULL,
                valor VARCHAR(MAX) NOT NULL,
                created_at DATETIME DEFAULT GETDATE(),
                updated_at DATETIME DEFAULT GETDATE(),
                is_active BIT DEFAULT 1,
                CONSTRAINT FK_entidades_contrato FOREIGN KEY (contrato_id) REFERENCES Contrato(id)
            );
            """
        ]
        
        # Create tables
        for command in table_commands:
            try:
                cursor.execute(command)
                conn.commit()
                logger.info("Successfully created and committed table")
            except Exception as create_error:
                logger.error(f"Error creating table: {str(create_error)}")
                logger.error(f"Failed command: {command}")
                raise create_error

        # Create indexes
        index_commands = [
            "CREATE INDEX idx_contrato_tipo ON Contrato(tipo_contrato, tipo_servicio) WHERE is_active = 1;",
            "CREATE INDEX idx_contrato_fechas ON Contrato(fecha_inicio, fecha_termino) WHERE is_active = 1;",
            "CREATE INDEX idx_multas_contrato ON Multas(contrato_id) WHERE is_active = 1;",
            "CREATE INDEX idx_multas_tipo ON Multas(tipo_incumplimiento) WHERE is_active = 1;",
            "CREATE INDEX idx_compania_rut ON CompaniaInfo(rut) WHERE is_active = 1;",
            "CREATE INDEX idx_proveedores_rut ON ProveedoresInfo(rut) WHERE is_active = 1;",
            "CREATE INDEX idx_representantes_cedula ON Representantes(cedula_de_identidad, contrato_id) WHERE is_active = 1;",
            "CREATE INDEX idx_entidades_tipo ON Entidades(tipo, contrato_id) WHERE is_active = 1;"
        ]

        # Create indexes
        for command in index_commands:
            try:
                cursor.execute(command)
                conn.commit()
                logger.info("Successfully created and committed index")
            except Exception as index_error:
                logger.error(f"Error creating index: {str(index_error)}")
                logger.error(f"Failed command: {command}")
                continue

        logger.info("All tables and indexes created successfully")
        return True

    except Exception as e:
        logger.error(f"Error in create_tables: {str(e)}")
        raise e
    finally:
        if cursor:
            cursor.close()

def verify_table_exists(cursor, table_name):
    """Verify if a table exists in the database."""
    cursor.execute(f"SELECT COUNT(*) FROM sys.tables WHERE name = '{table_name}'")
    return cursor.fetchone()[0] > 0

def verify_tables_exist(conn):
    """Verify that all required tables exist."""
    try:
        cursor = conn.cursor()
        required_tables = ['Contrato', 'CompaniaInfo', 'ProveedoresInfo', 'Representantes', 'Entidades']
        missing_tables = []
        
        for table in required_tables:
            if not verify_table_exists(cursor, table):
                missing_tables.append(table)
        
        if missing_tables:
            logger.error(f"Missing tables: {', '.join(missing_tables)}")
            return False
        
        logger.info("All required tables exist")
        return True
        
    except Exception as e:
        logger.error(f"Error verifying tables: {str(e)}")
        return False

# Helper function was moved to the top of the file

def insert_contract_data(contract_data):
    """
    Insert contract data and all related information into the database.    
    Args:
        contract_data (dict): Dictionary containing all contract data sections
            including Contrato, CompaniaInfo, ProveedoresInfo, Representantes, and Entidades.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Insert main contract data
                contrato_data = contract_data.get('Contrato', {})
                cursor.execute("""
                    INSERT INTO Contrato (
                        tipo_contrato, tipo_servicio, parte_cliente, parte_proveedor,
                        fecha_inicio, fecha_termino, renovacion_automatica, monto_total, 
                        multa_monto, multa_penalidades, termino_anticipado_activo, 
                        termino_anticipado_plazo_dias, exclusividad_activo, exclusividad_detalles, 
                        descripcion, nombre, created_at, updated_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE()
                    )
                """, (
                    contrato_data.get('tipo_contrato', ''),
                    contrato_data.get('tipo_servicio', ''),
                    contrato_data.get('parte_cliente', ''),
                    contrato_data.get('parte_proveedor', ''),
                    validate_and_normalize_date(contrato_data.get('fecha_inicio')),
                    validate_and_normalize_date(contrato_data.get('fecha_termino')),
                    contrato_data.get('renovacion_automatica', False),
                    validate_and_normalize_decimal(contrato_data.get('monto_total')),
                    validate_and_normalize_decimal(contrato_data.get('multa_monto')),
                    contrato_data.get('multa_penalidades', ''),
                    contrato_data.get('termino_anticipado_activo', False),
                    contrato_data.get('termino_anticipado_plazo_dias'),
                    contrato_data.get('exclusividad_activo', False),
                    contrato_data.get('exclusividad_detalles', ''),
                    contrato_data.get('descripcion', ''),
                    contrato_data.get('nombre', '')
                ))
                
                # Get the generated contract ID
                cursor.execute("SELECT @@IDENTITY")
                contrato_id = cursor.fetchone()[0]
                
                # Insert related information using helper functions
                insert_company_info(cursor, contrato_id, contract_data.get('CompaniaInfo'))
                insert_proveedor_info(cursor, contrato_id, contract_data.get('ProveedoresInfo'))
                insert_representantes(cursor, contrato_id, contract_data.get('Representantes', []))
                # Insert all entities from the EntidadesList if available, otherwise use single Entidades
                entidades_to_insert = contract_data.get('EntidadesList', [])
                if not entidades_to_insert:
                    # Fallback to single Entidades for backward compatibility
                    single_entidades = contract_data.get('Entidades', {})
                    if single_entidades:
                        entidades_to_insert = [single_entidades]
                
                insert_entidades(cursor, contrato_id, entidades_to_insert)
                insert_multas(cursor, contrato_id, contract_data.get('Multas', []))
                
                # Commit all changes
                conn.commit()
                
                return contrato_id
                
    except Exception as e:
        logging.error(f"Error inserting contract data: {e}")
        raise

def validate_table_schema(conn, table_name, expected_columns):
    """Validate the schema of a specific table."""
    try:
        cursor = conn.cursor()
        
        # Query column information
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
        """, (table_name,))
        
        actual_columns = {row[0]: {
            'data_type': row[1],
            'max_length': row[2],
            'nullable': row[3] == 'YES'
        } for row in cursor.fetchall()}
        
        # Check for missing columns
        missing_columns = set(expected_columns.keys()) - set(actual_columns.keys())
        if missing_columns:
            logger.error(f"Missing columns in {table_name}: {missing_columns}")
            return False
            
        # Check for column type mismatches
        for col_name, expected in expected_columns.items():
            if col_name not in actual_columns:
                continue
                
            actual = actual_columns[col_name]
            if expected['data_type'].lower() != actual['data_type'].lower():
                logger.error(f"Column type mismatch in {table_name}.{col_name}: "
                           f"expected {expected['data_type']}, got {actual['data_type']}")
                return False
                
        logger.info(f"Schema validation successful for table {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error validating schema for {table_name}: {str(e)}")
        return False

def validate_all_schemas(conn):
    """Validate schemas for all tables."""
    schemas = {
        'Contrato': {
            'id': {'data_type': 'int', 'nullable': False},
            'tipo_contrato': {'data_type': 'varchar', 'nullable': True},
            'tipo_servicio': {'data_type': 'varchar', 'nullable': True},
            'parte_cliente': {'data_type': 'varchar', 'nullable': True},
            'parte_proveedor': {'data_type': 'varchar', 'nullable': True},
            'fecha_inicio': {'data_type': 'date', 'nullable': True},
            'fecha_termino': {'data_type': 'date', 'nullable': True},
            'renovacion_automatica': {'data_type': 'bit', 'nullable': True},
            'monto_total': {'data_type': 'decimal', 'nullable': True},
            'multa_monto': {'data_type': 'decimal', 'nullable': True},
            'multa_penalidades': {'data_type': 'varchar', 'nullable': True},
            'termino_anticipado_activo': {'data_type': 'bit', 'nullable': True},
            'termino_anticipado_plazo_dias': {'data_type': 'int', 'nullable': True},
            'exclusividad_activo': {'data_type': 'bit', 'nullable': True},
            'exclusividad_detalles': {'data_type': 'varchar', 'nullable': True},
            'descripcion': {'data_type': 'varchar', 'nullable': True},
            'nombre': {'data_type': 'varchar', 'nullable': True},
            'created_at': {'data_type': 'datetime', 'nullable': True},
            'updated_at': {'data_type': 'datetime', 'nullable': True},
            'is_active': {'data_type': 'bit', 'nullable': True}
        },
        'Multas': {
            'id': {'data_type': 'int', 'nullable': False},
            'contrato_id': {'data_type': 'int', 'nullable': False},
            'tipo_incumplimiento': {'data_type': 'varchar', 'nullable': False},
            'implicancias': {'data_type': 'varchar', 'nullable': True},
            'monto_multa_uf': {'data_type': 'varchar', 'nullable': True},
            'plazo_constancia': {'data_type': 'varchar', 'nullable': True},
            'descripcion_completa': {'data_type': 'varchar', 'nullable': True},
            'created_at': {'data_type': 'datetime', 'nullable': True},
            'updated_at': {'data_type': 'datetime', 'nullable': True},
            'is_active': {'data_type': 'bit', 'nullable': True}
        },
        'CompaniaInfo': {
            'id': {'data_type': 'int', 'nullable': False},
            'contrato_id': {'data_type': 'int', 'nullable': False},
            'nombre': {'data_type': 'varchar', 'nullable': True},
            'rut': {'data_type': 'varchar', 'nullable': True},
            'domicilio': {'data_type': 'varchar', 'nullable': True},
            'created_at': {'data_type': 'datetime', 'nullable': True},
            'updated_at': {'data_type': 'datetime', 'nullable': True},
            'is_active': {'data_type': 'bit', 'nullable': True}
        },
        'ProveedoresInfo': {
            'id': {'data_type': 'int', 'nullable': False},
            'contrato_id': {'data_type': 'int', 'nullable': False},
            'nombre': {'data_type': 'varchar', 'nullable': True},
            'rut': {'data_type': 'varchar', 'nullable': True},
            'domicilio': {'data_type': 'varchar', 'nullable': True},
            'created_at': {'data_type': 'datetime', 'nullable': True},
            'updated_at': {'data_type': 'datetime', 'nullable': True},
            'is_active': {'data_type': 'bit', 'nullable': True}
        },
        'Representantes': {
            'id': {'data_type': 'int', 'nullable': False},
            'contrato_id': {'data_type': 'int', 'nullable': False},
            'nombre': {'data_type': 'varchar', 'nullable': False},
            'cedula_de_identidad': {'data_type': 'varchar', 'nullable': False},
            'created_at': {'data_type': 'datetime', 'nullable': True},
            'updated_at': {'data_type': 'datetime', 'nullable': True},
            'is_active': {'data_type': 'bit', 'nullable': True}
        },
        'Entidades': {
            'id': {'data_type': 'int', 'nullable': False},
            'contrato_id': {'data_type': 'int', 'nullable': False},
            'tipo': {'data_type': 'varchar', 'nullable': False},
            'valor': {'data_type': 'varchar', 'nullable': False},
            'created_at': {'data_type': 'datetime', 'nullable': True},
            'updated_at': {'data_type': 'datetime', 'nullable': True},
            'is_active': {'data_type': 'bit', 'nullable': True}
        }
    }
    
    success = True
    for table_name, expected_columns in schemas.items():
        if not validate_table_schema(conn, table_name, expected_columns):
            success = False
            
    return success

def drop_all_foreign_keys(conn):
    """Drop all foreign key constraints in the database."""
    try:
        cursor = conn.cursor()
        
        # Get all foreign key constraints
        fk_query = """
            SELECT 
                OBJECT_NAME(fk.parent_object_id) AS TableName,
                fk.name AS FKConstraintName
            FROM sys.foreign_keys fk
            ORDER BY TableName
        """
        
        cursor.execute(fk_query)
        constraints = cursor.fetchall()
        
        # Drop each foreign key constraint
        for table_name, constraint_name in constraints:
            try:
                drop_command = f"ALTER TABLE {table_name} DROP CONSTRAINT {constraint_name}"
                cursor.execute(drop_command)
                conn.commit()
                logger.info(f"Dropped foreign key {constraint_name} from {table_name}")
            except Exception as e:
                logger.warning(f"Error dropping foreign key {constraint_name}: {str(e)}")
                continue
                
        logger.info("Completed foreign key constraint cleanup")
        return True
        
    except Exception as e:
        logger.error(f"Error in drop_all_foreign_keys: {str(e)}")
        return False

def create_foreign_keys():
    """Create foreign key relationships between tables."""
    fk_query = """
        ALTER TABLE contratos_metadata 
        ADD CONSTRAINT FK_contratos_metadata_contratos 
        FOREIGN KEY (contrato_id) REFERENCES Contrato (id);
        
        ALTER TABLE Representantes
        ADD CONSTRAINT FK_representantes_contratos
        FOREIGN KEY (contrato_id) REFERENCES Contrato (id);
        
        ALTER TABLE Entidades
        ADD CONSTRAINT FK_entidades_contratos
        FOREIGN KEY (contrato_id) REFERENCES Contrato (id);
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(fk_query)
            conn.commit()
        logging.info("Foreign keys created successfully")
    except Exception as e:
        logging.error(f"Error creating foreign keys: {e}")
        raise

def insert_company_info(cursor, contrato_id, company_data):
    """
    Insert company information.
    
    Args:
        cursor: Database cursor
        contrato_id: ID of the parent contract
        company_data: Dictionary containing company info (Nombre, RUT, Domicilio)
    """
    if not company_data:
        return
    
    try:
        cursor.execute("""
            INSERT INTO CompaniaInfo (
                contrato_id, nombre, rut, domicilio, created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, GETDATE(), GETDATE()
            )
        """, (
            contrato_id,
            company_data.get('nombre', ''),
            company_data.get('rut', ''),
            company_data.get('domicilio', '')
        ))
        
        logger.info(f"Successfully inserted company info for contract {contrato_id}")
        cursor.execute("SELECT @@IDENTITY")
        return cursor.fetchone()[0]
        
    except Exception as e:
        logger.error(f"Error inserting company: {str(e)}")
        logger.error(f"Company data: {company_data}")
        raise

def insert_proveedor_info(cursor, contrato_id, proveedores_data):
    """
    Insert provider information.
    
    Args:
        cursor: Database cursor
        contrato_id: ID of the parent contract
        proveedores_data: List of dictionaries containing provider info (Nombre, RUT, Domicilio)
    """
    if not proveedores_data:
        return    
    # Handle both single dict and list of dicts
    if not isinstance(proveedores_data, list):
        proveedores_data = [proveedores_data]
        
    provider_ids = []
    for proveedor in proveedores_data:
        try:
            # Debug logging to see what data we're getting
            nombre = proveedor.get('nombre', '')
            rut = proveedor.get('rut', '')
            domicilio = proveedor.get('domicilio', '')
            
            logger.info(f"Inserting provider: nombre='{nombre}', rut='{rut}', domicilio='{domicilio}'")
            logger.info(f"Raw provider data: {proveedor}")
            
            cursor.execute("""
                INSERT INTO ProveedoresInfo (
                    contrato_id, nombre, rut, domicilio, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, GETDATE(), GETDATE()
                )
            """, (
                contrato_id,
                nombre,
                rut,
                domicilio
            ))
            
            cursor.execute("SELECT @@IDENTITY")
            provider_id = cursor.fetchone()[0]
            provider_ids.append(provider_id)
            logger.info(f"Successfully inserted provider info for contract {contrato_id}: {nombre} with ID {provider_id}")
            
        except Exception as e:
            logger.error(f"Error inserting provider: {str(e)}")
            logger.error(f"Provider data: {proveedor}")
            raise
            
    return provider_ids

def insert_representantes(cursor, contrato_id, representantes_data):
    """Insert representatives information."""
    if not representantes_data:
        return
    
    for rep in representantes_data:
        cursor.execute("""            INSERT INTO Representantes (
                contrato_id, nombre, cedula_de_identidad, created_at, updated_at
            ) VALUES (
                ?, ?, ?, GETDATE(), GETDATE()
            )
        """, (
            contrato_id,
            rep.get('nombre', ''),
            rep.get('cedula_identidad', '')
        ))

def insert_entidades(cursor, contrato_id, entidades_data):
    """Insert entities with the simplified tipo and valor structure."""
    if not entidades_data:
        return
    
    # Helper function to safely get string values
    def safe_get_string(data, key, default=''):
        value = data.get(key, default)
        if value is None:
            return default
        return str(value).strip()
    
    # Convert dictionary of entities to list if needed
    if isinstance(entidades_data, dict):
        entidades_list = [entity for entity in entidades_data.values() if isinstance(entity, dict)]
    else:
        entidades_list = entidades_data if isinstance(entidades_data, list) else []

    for entidad in entidades_list:
        if not isinstance(entidad, dict):
            continue
            
        try:
                        # Get tipo and valor with null safety
            tipo = safe_get_string(entidad, 'tipo')
            valor = safe_get_string(entidad, 'valor')            
            # Skip if both tipo and valor are empty
            if not tipo and not valor:
                logger.warning("Skipping entity with missing tipo and valor")
                continue
                
            cursor.execute("""
                INSERT INTO Entidades (
                    contrato_id, tipo, valor, created_at, updated_at
                ) VALUES (?, ?, ?, GETDATE(), GETDATE())
            """, (
                contrato_id,
                tipo,
                valor
            ))
            
            logger.info(f"Successfully inserted entity: {tipo} - {valor}")
            
        except Exception as e:
            logger.error(f"Error inserting entity {entidad}: {str(e)}")
            continue

def insert_multas(cursor, contrato_id, multas_data):
    """
    Insert multas (fines/penalties) records for a contract.
    
    Args:
        cursor: Database cursor
        contrato_id: ID of the parent contract
        multas_data: List of dictionaries containing multa information
    """
    if not multas_data or not isinstance(multas_data, list):
        logger.warning("No multas array found in contract data")
        return []

    multa_ids = []
    
    # Helper function to safely get string values
    def safe_get_string(data, key, default=''):
        value = data.get(key, default)
        if value is None:
            return default
        return str(value).strip()

    for multa in multas_data:
        try:
            if not isinstance(multa, dict):
                logger.warning(f"Skipping invalid multa data format: {multa}")
                continue
                
            # Get tipo_incumplimiento (maps to tipo_infraccion in DB)
            tipo_incumplimiento = multa.get('tipo_incumplimiento', '')
            if tipo_incumplimiento is None:
                tipo_incumplimiento = ''
            else:
                tipo_incumplimiento = str(tipo_incumplimiento).strip()
                
            if not tipo_incumplimiento:
                logger.warning("Skipping multa with missing tipo_incumplimiento")
                continue
                
            # Convert monto to decimal if present
            monto_uf = None
            if 'monto_multa_uf' in multa and multa['monto_multa_uf'] is not None:
                try:
                    monto_str = str(multa['monto_multa_uf']).strip()
                    if monto_str and monto_str.lower() != 'null':
                        # Remove any non-numeric characters except decimal point
                        import re
                        monto_clean = re.sub(r'[^\d.,]', '', monto_str)
                        monto_clean = monto_clean.replace(',', '.')
                        if monto_clean:
                            monto_uf = float(monto_clean)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse monto_multa_uf '{multa['monto_multa_uf']}': {e}")
              # Insert the multa (correct table and field names)
            cursor.execute("""
                INSERT INTO Multas (
                    contrato_id, tipo_incumplimiento, implicancias,
                    monto_multa_uf, plazo_constancia, descripcion_completa,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())
            """, (
                contrato_id,
                tipo_incumplimiento,
                safe_get_string(multa, 'implicancias'),
                monto_uf,
                safe_get_string(multa, 'plazo_constancia'),
                safe_get_string(multa, 'descripcion_completa')
            ))
            
            cursor.execute("SELECT @@IDENTITY")
            multa_id = cursor.fetchone()[0]
            multa_ids.append(multa_id)
            logger.info(f"Successfully inserted multa {tipo_incumplimiento} with ID {multa_id}")
            
        except Exception as e:
            logger.error(f"Error inserting multa: {str(e)}")
            logger.error(f"Multa data: {multa}")
            continue
            
    return multa_ids