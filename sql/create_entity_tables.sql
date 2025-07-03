-- Create Entidades table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[entidades]') AND type in (N'U'))
BEGIN
    CREATE TABLE entidades (
        entidad_id INT IDENTITY(1,1) PRIMARY KEY,
        contrato_id INT NOT NULL,
        tipo_entidad NVARCHAR(255) NOT NULL,
        nombre NVARCHAR(255) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        is_active BIT NOT NULL DEFAULT 1,
        CONSTRAINT FK_entidades_contratos FOREIGN KEY (contrato_id) REFERENCES contratos(contrato_id)
    );

    -- Create index for faster lookups by tipo_entidad and nombre
    CREATE INDEX idx_entidades_tipo ON entidades(tipo_entidad, nombre);
    CREATE INDEX idx_entidades_contrato ON entidades(contrato_id);

    PRINT 'Created table: entidades';
END
ELSE
BEGIN
    PRINT 'Table already exists: entidades';
END

-- Create AtributosDeEntidad table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[atributos_entidad]') AND type in (N'U'))
BEGIN
    CREATE TABLE atributos_entidad (
        atributo_id INT IDENTITY(1,1) PRIMARY KEY,
        entidad_id INT NOT NULL,
        contrato_id INT NOT NULL,
        nombre_atributo NVARCHAR(255) NOT NULL,
        valor NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        is_active BIT NOT NULL DEFAULT 1,
        CONSTRAINT FK_atributos_entidades FOREIGN KEY (entidad_id) REFERENCES entidades(entidad_id),
        CONSTRAINT FK_atributos_contratos FOREIGN KEY (contrato_id) REFERENCES contratos(contrato_id)
    );

    -- Create indexes for faster lookups
    CREATE INDEX idx_atributos_entidad ON atributos_entidad(entidad_id);
    CREATE INDEX idx_atributos_contrato ON atributos_entidad(contrato_id);
    CREATE INDEX idx_atributos_nombre ON atributos_entidad(nombre_atributo);

    PRINT 'Created table: atributos_entidad';
END
ELSE
BEGIN
    PRINT 'Table already exists: atributos_entidad';
END

-- Create trigger for updating timestamps on entidades
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR_UpdateTimestamp_entidades')
BEGIN
    EXEC('CREATE TRIGGER TR_UpdateTimestamp_entidades ON entidades AFTER UPDATE AS 
    BEGIN
        UPDATE entidades SET updated_at = GETDATE()
        FROM entidades t
        INNER JOIN inserted i ON t.entidad_id = i.entidad_id
    END');
    
    PRINT 'Created trigger: TR_UpdateTimestamp_entidades';
END
ELSE
BEGIN
    PRINT 'Trigger already exists: TR_UpdateTimestamp_entidades';
END

-- Create trigger for updating timestamps on atributos_entidad
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR_UpdateTimestamp_atributos_entidad')
BEGIN
    EXEC('CREATE TRIGGER TR_UpdateTimestamp_atributos_entidad ON atributos_entidad AFTER UPDATE AS 
    BEGIN
        UPDATE atributos_entidad SET updated_at = GETDATE()
        FROM atributos_entidad t
        INNER JOIN inserted i ON t.atributo_id = i.atributo_id
    END');
    
    PRINT 'Created trigger: TR_UpdateTimestamp_atributos_entidad';
END
ELSE
BEGIN
    PRINT 'Trigger already exists: TR_UpdateTimestamp_atributos_entidad';
END
