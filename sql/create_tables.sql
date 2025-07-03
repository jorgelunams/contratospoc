-- Create Contratos table
CREATE TABLE contratos (
    contrato_id INT IDENTITY(1,1) PRIMARY KEY,
    tipo_contrato NVARCHAR(255) NOT NULL,
    tipo_servicio NVARCHAR(255) NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_termino DATE NOT NULL,
    renovacion_automatica BIT NOT NULL DEFAULT 0,
    monto_total DECIMAL(19,4) NULL,
    condiciones_pago NVARCHAR(MAX) NULL,
    plazo_pago_dias INT NULL,
    termino_anticipado_dias INT NULL,
    exclusividad BIT NOT NULL DEFAULT 0,
    detalles_exclusividad NVARCHAR(MAX) NULL,
    ley_aplicable NVARCHAR(255) NULL,
    domicilio_jurisdiccion NVARCHAR(255) NULL,
    -- Metadata fields
    nombre_documento NVARCHAR(255) NULL,
    numero_pagina INT NULL,
    anexos_incluidos NVARCHAR(MAX) NULL,
    referencia_interna NVARCHAR(255) NULL,
    observacion_paginas NVARCHAR(MAX) NULL,
    numero_tokens INT NULL,
    total_palabras INT NULL,
    total_anexos INT NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    is_active BIT NOT NULL DEFAULT 1
);

-- Create index for contratos
CREATE INDEX idx_contratos_fecha_inicio ON contratos(fecha_inicio);
CREATE INDEX idx_contratos_fecha_termino ON contratos(fecha_termino);
CREATE INDEX idx_contratos_tipo ON contratos(tipo_contrato, tipo_servicio);

-- Create indexes for metadata fields
CREATE INDEX idx_contratos_metadata_nombre ON contratos(nombre_documento) WHERE is_active = 1;
CREATE INDEX idx_contratos_metadata_ref ON contratos(referencia_interna) WHERE is_active = 1;
GO

-- Create Penalidades table
CREATE TABLE penalidades (
    penalidad_id INT IDENTITY(1,1) PRIMARY KEY,
    contrato_id INT NOT NULL,
    descripcion NVARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    is_active BIT NOT NULL DEFAULT 1,
    CONSTRAINT FK_penalidades_contratos FOREIGN KEY (contrato_id) REFERENCES contratos(contrato_id)
);

-- Create Partes table
CREATE TABLE partes (
    parte_id INT IDENTITY(1,1) PRIMARY KEY,
    contrato_id INT NOT NULL,
    nombre NVARCHAR(255) NOT NULL,
    rut NVARCHAR(50) NOT NULL,
    domicilio NVARCHAR(255) NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    is_active BIT NOT NULL DEFAULT 1,
    CONSTRAINT FK_partes_contratos FOREIGN KEY (contrato_id) REFERENCES contratos(contrato_id)
);

-- Create index for RUT
CREATE UNIQUE INDEX idx_partes_rut ON partes(rut) WHERE is_active = 1;

-- Create Representantes table
CREATE TABLE representantes (
    representante_id INT IDENTITY(1,1) PRIMARY KEY,
    contrato_id INT NOT NULL,
    nombre NVARCHAR(255) NOT NULL,
    cedula_de_identidad NVARCHAR(50) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    is_active BIT NOT NULL DEFAULT 1,
    CONSTRAINT FK_representantes_contratos FOREIGN KEY (contrato_id) REFERENCES contratos(contrato_id)
);

-- Create index for cedula_de_identidad and contract
CREATE INDEX idx_representantes_cedula ON representantes(cedula_de_identidad, contrato_id) WHERE is_active = 1;

-- Create AdministradoresDeContrato table
CREATE TABLE administradores_contrato (
    administrador_id INT IDENTITY(1,1) PRIMARY KEY,
    contrato_id INT NOT NULL,
    nombre NVARCHAR(255) NOT NULL,
    telefono NVARCHAR(50) NULL,
    correo_electronico NVARCHAR(255) NOT NULL,
    direccion NVARCHAR(255) NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    is_active BIT NOT NULL DEFAULT 1,
    CONSTRAINT FK_administradores_contratos FOREIGN KEY (contrato_id) REFERENCES contratos(contrato_id)
);

-- Create index for correo_electronico
CREATE UNIQUE INDEX idx_administradores_correo ON administradores_contrato(correo_electronico) WHERE is_active = 1;

-- Create Entidades table
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

-- Create AtributosDeEntidad table
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

-- Create MultasAsociadas table
CREATE TABLE multas_asociadas (
    multa_id INT IDENTITY(1,1) PRIMARY KEY,
    contrato_id INT NOT NULL,
    tipo_infraccion NVARCHAR(255) NOT NULL,
    implicancias NVARCHAR(MAX) NULL,
    monto_en_uf DECIMAL(10,2) NULL,
    plazo_para_constancia NVARCHAR(MAX) NULL,
    descripcion_completa NVARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    is_active BIT NOT NULL DEFAULT 1,
    CONSTRAINT FK_multas_contratos FOREIGN KEY (contrato_id) REFERENCES contratos(contrato_id)
);

-- Create trigger for updated_at
CREATE TRIGGER TR_UpdateTimestamp_contratos ON contratos AFTER UPDATE AS 
BEGIN
    UPDATE contratos SET updated_at = GETDATE()
    FROM contratos t
    INNER JOIN inserted i ON t.contrato_id = i.contrato_id
END;

-- Add similar triggers for other tables
GO

CREATE TRIGGER TR_UpdateTimestamp_penalidades ON penalidades AFTER UPDATE AS 
BEGIN
    UPDATE penalidades SET updated_at = GETDATE()
    FROM penalidades t
    INNER JOIN inserted i ON t.penalidad_id = i.penalidad_id
END;
GO

-- Add similar triggers for all other tables

-- Create trigger to handle metadata updates
CREATE TRIGGER TR_UpdateMetadataTimestamp_contratos ON contratos AFTER UPDATE AS 
BEGIN
    IF UPDATE(nombre_documento) OR 
       UPDATE(numero_pagina) OR 
       UPDATE(anexos_incluidos) OR 
       UPDATE(referencia_interna) OR 
       UPDATE(observacion_paginas) OR 
       UPDATE(numero_tokens) OR 
       UPDATE(total_palabras) OR 
       UPDATE(total_anexos)
    BEGIN
        UPDATE contratos SET updated_at = GETDATE()
        FROM contratos t
        INNER JOIN inserted i ON t.contrato_id = i.contrato_id
    END
END;
GO
